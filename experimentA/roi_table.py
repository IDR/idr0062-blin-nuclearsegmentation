import os
import omero.cli
import pandas
from omero.gateway import BlitzGateway
from omero_metadata.populate import ParsingContext


PROJECT = "idr0062-blin-nuclearsegmentation/experimentA"
TABLE_NAME = "features"


def get_features_table(dataset):
    ann = next(dataset.listAnnotations())
    file_path = os.path.join("/tmp", ann.getFile().getName())

    with open(str(file_path), 'wb') as f:
        for chunk in ann.getFileInChunks():
            f.write(chunk)

    df = pandas.read_csv(file_path, delimiter="\t")
    return df


def iter_rois(roi_service, dataset):
    for image in dataset.listChildren():
        result = roi_service.findByImage(image.getId(), None)
        for roi in result.rois:
            for shape in roi.copyShapes():
                yield image, roi, shape


def delete_tables(conn, dataset):
    for ann in dataset.listAnnotations(ns="openmicroscopy.org/omero/bulk_annotations"):
        conn.deleteObject(ann._obj)


def populate_metadata(conn, target_obj, file_path, column_types):
    ctx = ParsingContext(
        conn.c, target_obj._obj, file=file_path, allow_nan=True,
        table_name=TABLE_NAME, column_types=column_types
    )
    ctx.parse()


def get_column_types(df):
    column_types = []
    for rowIndex, row in df.iterrows():
        for columnIndex, value in row.items():
            if type(value) == int:
                column_types.append("l")
            elif type(value) == float:
                column_types.append("d")
            elif type(value) == bool:
                column_types.append("b")
            else:
                column_types.append("s")
        break
    return column_types


def handle_dataset(conn, dataset_id):
    dataset = conn.getObject('Dataset', attributes={'id': dataset_id})
    try:
        print("Delete old tables")
        delete_tables(conn, dataset)
    except:
        pass

    print("Update CSV")
    roi_service = conn.getRoiService()
    features_table = get_features_table(dataset)
    column_types = get_column_types(features_table)
    features_table["roi"] = -1
    features_table["shape"] = -1
    features_table["Image Name"] = "NA"
    column_types.append("roi")
    column_types.append("l")
    column_types.append("s")
    for img, roi, shape in iter_rois(roi_service, dataset):
        label = float(f"{roi._name._val}.0")
        if "Image_Name" in features_table:
            img_name = img.getName().replace(".tif", ".ids")
            features_table.loc[(features_table['Image_Name'] == img_name) & (features_table['label'] == label), 'roi'] = roi._id._val
            features_table.loc[(features_table['Image_Name'] == img_name) & (features_table['label'] == label), 'shape'] = shape._id._val
            features_table.loc[(features_table['Image_Name'] == img_name) & (features_table['label'] == label), 'Image Name'] = img.getName()
        else:
            features_table.loc[(features_table['label'] == label), 'roi'] = roi._id._val
            features_table.loc[(features_table['label'] == label), 'shape'] = shape._id._val
            features_table.loc[(features_table['label'] == label), 'Image Name'] = img.getName()
    features_table.drop(features_table[(features_table.roi == -1)].index, errors = 'ignore', inplace=True)
    tmp_file = "/tmp/tmp.csv"
    features_table.to_csv(tmp_file, sep=',', encoding='utf-8', index=False)
    print("Create table")
    populate_metadata(conn, dataset, tmp_file, column_types)


def main():
    with omero.cli.cli_login() as c:
        conn = BlitzGateway(client_obj=c.get_client())
        project = conn.getObject('Project', attributes={'name': PROJECT})
        datasets = list(project.listChildren())

    for dataset in datasets:
        # Have to create a new connection for each dataset because
        # somehow ParsingContext in populate_metadata messes up the
        # connection
        with omero.cli.cli_login() as c:
            conn = BlitzGateway(client_obj=c.get_client())
            print(f"Processing dataset {dataset.getName()}")
            try:
                handle_dataset(conn, dataset.getId())
            except Exception as e:
                print(e)
                pass


if __name__ == "__main__":
    main()
