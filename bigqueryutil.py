# pip install --upgrade google-cloud-bigquery
import google
import xlrd
from google.cloud import bigquery


def readschema(sheetname):
    loc = "config.xlsx"
    wb = xlrd.open_workbook(loc)
    sheet = wb.sheet_by_name(sheetname)
    newlist = list()
    for i in range(1, sheet.nrows):
        mode = sheet.row_values(i)[2] if sheet.row_values(i)[2] is not '' else 'NULLABLE'
        newlist.append(bigquery.SchemaField(sheet.row_values(i)[0], sheet.row_values(i)[1], mode=mode))
    return newlist


def create_table(table_id, project, dataset):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "{}.{}.{}".format(project, dataset, table_id)
    try:

        # SCHEMA = ','.join([
        #     'url:STRING',
        #     'num_reviews:INTEGER',
        #     'score:FLOAT64',
        #     'first_date:TIMESTAMP',
        #     'last_date:TIMESTAMP',
        #     ])
        schema = readschema('schema')
        # print(listd)

        # schema = [
        #     bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
        #     bigquery.SchemaField("num_reviews", "INTEGER"),
        #     bigquery.SchemaField("score", "FLOAT"),
        #     bigquery.SchemaField("first_date", "TIMESTAMP"),
        #     bigquery.SchemaField("last_date", "TIMESTAMP"),
        #     ]

        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )
    except google.api_core.exceptions.Conflict as er:
        print('Table {} already present'.format(table_id))


def create_dataset(project, datasetID, location, tablename):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    dataset_id = "{}.{}".format(project, datasetID)
    try:
        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        # "US"
        # Send the dataset to the API for creation.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
        dataset = client.create_dataset(dataset)  # Make an API request.
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    except google.api_core.exceptions.Conflict as er:
        print('Dataset already present')
    create_table(tablename, project, datasetID)
