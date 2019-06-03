#!/usr/bin/env python

# WARNING: This script will use your current OMERO login session if one
# exists. If you want to use a different login ensure you logout first.

from glob import glob
import os
import pandas as pd

import omero.clients
import omero.cli
from omero_upload import upload_ln_s

DRYRUN = False
OMERO_DATA_DIR = '/data/OMERO'
NAMESPACE = 'openmicroscopy.org/idr/analysis/original'


def get_feature_files():
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
    project = conn.getObject('Project', attributes={
        'name': 'idr0062-blin-nuclearsegmentation/experimentA'})
    for dataset in project.listChildren():
        yield dataset


def get_images(conn):
    for dataset in get_datasets(conn):
        for image in dataset.listChildren():
            if not image.name.endswith('_Manual.tif'):
                yield image


def existing_file_attachments(obj):
    files = set()
    for ann in obj.listAnnotations():
        try:
            f = ann.getFile()
            files.add(f.name)
        except AttributeError:
            continue
    return files


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

        if seg in existing:
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
            fo = conn.c.upload(features, 'text/tab-separated-values')
            fa = omero.model.FileAnnotationI()
            fa.setFile(fo._obj)
            fa.setNs(omero.rtypes.rstring(NAMESPACE))
            fa = conn.getUpdateService().saveAndReturnObject(fa)
            fa = omero.gateway.FileAnnotationWrapper(conn, fa)
            ds.linkAnnotation(fa)


if __name__ == '__main__':
    with omero.cli.cli_login() as c:
        conn = omero.gateway.BlitzGateway(client_obj=c.get_client())
        main(conn)
