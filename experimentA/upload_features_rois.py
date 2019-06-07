#!/usr/bin/env python
# coding: utf-8

# WARNING: This script will use your current OMERO login session if one
# exists. If you want to use a different login ensure you logout first.

from glob import glob
import numpy as np
import os
import pandas as pd
from skimage.io import imread

import omero.clients
import omero.cli
from omero.rtypes import (
    rstring,
)
from omero_rois import (
    mask_from_binary_image,
    NoMaskFound,
)
from omero_upload import upload_ln_s

DRYRUN = False
SKIP_IF_EXISTING_ROIS = True
PROJECT_NAME = 'idr0062-blin-nuclearsegmentation/experimentA'
OMERO_DATA_DIR = '/data/OMERO'
NAMESPACE = 'openmicroscopy.org/idr/analysis/original'
RGBA = (128, 128, 128, 128)


def get_feature_files():
    """
    Returns mappings of:
    - image names to manual segmentations
    - dataset names to feature TSVs
    """
    fileset_root = (
        '/uod/idr/filesets/idr0062-blin-nuclearsegmentation/20190429-ftp')
    labels = sorted(glob('*_labels.txt'))
    dfs = [pd.read_csv(f, sep='\t') for f in labels]
    df = pd.concat(dfs, axis=0, sort=True)
    # Check image names are unique so we can skip checking datasets
    print(
        len(df['Segmented Image']),
        len(df['Segmented Image'].unique()),
        len(df['Source Name']),
        len(df['Source Name'].unique()),
    )

    image_segmentation_map = dict(
        zip(df['Segmented Image'], df['Comment [Image File Path]']))
    image_attachment_map = dict(
        (k, fileset_root + v) for (k, v) in image_segmentation_map.iteritems())

    dataset_experiment_map = {
        'Acini': 'idr0000-experimentB',
        'Blastocysts': 'idr0000-experimentC',
        'E75': 'idr0000-experimentD',
        'E875': 'idr0000-experimentE',
        'Neural': 'idr0000-experimentA',
    }
    dataset_attachment_map = {}
    for (dataset, prefix) in dataset_experiment_map.iteritems():
        dataset_attachment_map[dataset] = prefix + '_features.tsv'

    for v in image_attachment_map.values():
        assert os.path.exists(v), v
    for v in dataset_attachment_map.values():
        assert os.path.exists(v), v

    return image_attachment_map, dataset_attachment_map


def get_datasets(conn):
    """
    Get all datasets
    """
    project = conn.getObject('Project', attributes={'name': PROJECT_NAME})
    for dataset in project.listChildren():
        yield dataset


def get_images(conn):
    """
    Get all images
    """
    for dataset in get_datasets(conn):
        for image in dataset.listChildren():
            if not image.name.endswith('_Manual.tif'):
                yield image


def existing_file_attachments(obj):
    """
    Get existing file annotations
    """
    files = set()
    for ann in obj.listAnnotations():
        try:
            f = ann.getFile()
            files.add(f.name)
        except AttributeError:
            continue
    return files


def get_labels(im, image_attachment_map):
    """
    Get the manual segmentation for an image
    """
    # Currently broken until https://github.com/ome/omero-server/pull/16
    # is deployed to the IDR
    # anns = [a for a in im.listAnnotations(NAMESPACE)
    #         if a.getFile().name.endswith('_Manual.tif')]
    # assert len(anns) == 1
    # with anns[0].getFile().asFileObj() as fo:
    #     seg = imread(fo)
    seg = imread(image_attachment_map[im.name])
    segint = seg.astype(int)
    assert np.all(np.equal(seg, segint))
    assert segint.min() == 0
    return segint


def create_rois(im, labels):
    """
    Create single-Z masks for all labels and Zs.
    Combine all Zs for a given label into one ROI.
    """
    nz = im.getSizeZ()
    assert im.getSizeT() == 1
    if not all(np.equal(labels.shape,
               [im.getSizeZ(), im.getSizeY(), im.getSizeX()])):
        raise ValueError(
            'Incompatible ZYX dimensions image:{} labels:{}'.format(
                [im.getSizeZ(), im.getSizeY(), im.getSizeX()], labels.shape))

    rois = []
    ulabels = np.unique(labels)
    for n in range(1, len(ulabels)):
        print('Creating ROI index {}'.format(n))
        roi = omero.model.RoiI()
        roi.setName(rstring(n))
        for z in range(nz):
            try:
                mask = mask_from_binary_image(
                    labels[z] == ulabels[n], rgba=RGBA, z=z)
                roi.addShape(mask)
            except NoMaskFound:
                pass
        rois.append(roi)

    return rois


def save_rois(conn, im, rois):
    """
    Save ROIs on an image
    """
    print('Saving %d ROIs for image %d:%s' % (len(rois), im.id, im.name))
    us = conn.getUpdateService()
    for roi in rois:
        # Due to a bug need to reload the image for each ROI
        im = conn.getObject('Image', im.id)
        roi.setImage(im._obj)
        roi1 = us.saveAndReturnObject(roi)
        assert roi1


def main(conn):
    image_attachment_map, dataset_attachment_map = get_feature_files()

    errors = []
    for im in get_images(conn):
        print('Image: %d' % im.id)
        existing = existing_file_attachments(im)
        try:
            seg = image_attachment_map[im.name]
        except KeyError:
            errors.append('No segmentation found for {}'.format(im.name))
            continue

        if os.path.basename(seg) in existing:
            print('Skipping {} ➔ {}'.format(seg, im.name))
            continue

        print('Uploading (in-place) {} ➔ {}'.format(seg, im.name))
        if not DRYRUN:
            fo = upload_ln_s(conn.c, seg, OMERO_DATA_DIR, 'image/tiff')
            fa = omero.model.FileAnnotationI()
            fa.setFile(fo._obj)
            fa.setNs(omero.rtypes.rstring(NAMESPACE))
            fa = conn.getUpdateService().saveAndReturnObject(fa)
            fa = omero.gateway.FileAnnotationWrapper(conn, fa)
            im.linkAnnotation(fa)

    if errors:
        print('{} errors:'.format(len(errors)))
        for err in errors:
            print(err)

    for ds in get_datasets(conn):
        print('Dataset:{} {}'.format(ds.id, ds.name))
        existing = existing_file_attachments(ds)
        features = dataset_attachment_map[ds.name]

        if features in existing:
            print('Skipping {} ➔ {}'.format(features, ds.name))
            continue

        print('Uploading {} ➔ {}'.format(features, ds.name))
        if not DRYRUN:
            fo = conn.c.upload(features, type='text/tab-separated-values')
            fa = omero.model.FileAnnotationI()
            fa.setFile(omero.model.OriginalFileI(fo.id, False))
            fa.setNs(rstring(NAMESPACE))
            link = omero.model.DatasetAnnotationLinkI()
            link.setParent(ds._obj)
            link.setChild(fa)
            link = conn.getUpdateService().saveAndReturnObject(link)

    roisvc = conn.getRoiService()
    for im in get_images(conn):
        if SKIP_IF_EXISTING_ROIS and roisvc.findByImage(im.id, None).rois:
            print('Image:{} {} has ROIs, skipping'.format(im.id, im.name))
            continue
        print('Image:{} {}'.format(im.id, im.name))
        try:
            labels = get_labels(im, image_attachment_map)
            rois = create_rois(im, labels)
            if not DRYRUN:
                save_rois(conn, im, rois)
        except KeyError:
            print('No segmentation found for {}'.format(im.name))
        except ValueError as e:
            print('ERROR in {}: {}'.format(im.name, e))


if __name__ == '__main__':
    with omero.cli.cli_login() as c:
        conn = omero.gateway.BlitzGateway(client_obj=c.get_client())
        main(conn)
