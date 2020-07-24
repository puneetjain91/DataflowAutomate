import xlrd

from bigqueryutil import create_dataset
from cloudstorageutil import check_bucket_available
from pubsubutil import checktopicavailable, checksubscriptionavailable, publishmessage
import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window

# Defines the BigQuery schema for the output table.
SCHEMA = ','.join([
    'url:STRING',
    'num_reviews:INTEGER',
    'score:FLOAT64',
    'first_date:TIMESTAMP',
    'last_date:TIMESTAMP',
    ])


def parse_json_message(message):
    import time
    """Parse the input json message and add 'score' & 'processing_time' keys."""
    row = json.loads(message)
    return {
        'url': row['url'],
        'score': 1.0 if row['review'] == 'positive' else 0.0,
        'processing_time': int(time.time()),
        }


def get_statistics(url_messages):
    """Get statistics from the input URL messages."""
    url, messages = url_messages
    return {
        'url': url,
        'num_reviews': len(messages),
        'score': sum(msg['score'] for msg in messages) / len(messages),
        'first_date': min(msg['processing_time'] for msg in messages),
        'last_date': max(msg['processing_time'] for msg in messages),
        }


def run(args, project_id, job_id, region, input_subscription, output_table, temp_folder, window_interval):
    """to deploy in GCP dataflow"""

    # Create and set your PipelineOptions.
    # For Cloud execution, specify DataflowRunner and set the Cloud Platform
    # project, job name, temporary files location, and region.
    # For more information about regions, check:
    # https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
    options = PipelineOptions(
        flags=args,
        runner='DataflowRunner',
        project=project_id,
        job_name=job_id,
        temp_location=temp_folder,
        region=region,
        save_main_session=True,
        streaming=True)

    # Create the Pipeline with the specified options.
    # with beam.Pipeline(options=options) as pipeline:
    #   pass  # build your pipeline here.

    """Build and run the pipeline."""
    # options = PipelineOptions(args, save_main_session=True, streaming=True)
    pipeline = beam.Pipeline(options=options)
    #with beam.Pipeline(options=options) as pipeline:
    # Read the messages from PubSub and process them.
    messages = (
                pipeline
                | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(subscription=
                                                                input_subscription).with_output_types(bytes)
                | 'UTF-8 bytes to string' >> beam.Map(lambda msg: msg.decode('utf-8'))
                | 'Parse JSON messages' >> beam.Map(parse_json_message)
                | 'Fixed-size windows' >> beam.WindowInto(window.FixedWindows(int(window_interval), 0))
                | 'Add URL keys' >> beam.Map(lambda msg: (msg['url'], msg))
                | 'Group by URLs' >> beam.GroupByKey()
                | 'Get statistics' >> beam.Map(get_statistics))

    # Output the results into BigQuery table.
    output = messages | 'Write to Big Query' >> beam.io.WriteToBigQuery(
            output_table, schema=SCHEMA)

    result = pipeline.run()

def startdataflow(l):
    print('Dataflow starts .... :')
    # parser = argparse.ArgumentParser()
    # pipeline_args,known_args = parser.parse_known_args(l)
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project_id',
        help='Project_ID '
             'eg: peaceful-branch-279707')
    parser.add_argument(
        '--job_id',
        help='Unique Job ID')
    parser.add_argument(
        '--output_table',
        help='Output BigQuery table for results specified as: '
             'PROJECT:DATASET.TABLE or DATASET.TABLE.')
    parser.add_argument(
        '--input_subscription',
        help='Input PubSub subscription of the form '
             '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."')
    parser.add_argument(
        '--temp_folder',
        help='Temporary folder for data processing '
             '"gs://<BUCKET_ID>/<TEMP_FOLDER_ID>."')
    parser.add_argument(
        '--region',
        default='us-central1',
        help='region.')
    parser.add_argument(
        '--window_interval',
        default=60,
        help='Window interval in seconds for grouping incoming messages.')
    # known_args, pipeline_args = parser.parse_known_args()
    known_args, pipeline_args = parser.parse_known_args(l)
    print(known_args, '\n', pipeline_args)
    run(pipeline_args, known_args.project_id, known_args.job_id, known_args.region, known_args.input_subscription,
        known_args.output_table,
        known_args.temp_folder,
        known_args.window_interval)
    exit()


def readconfig(sheetname):
    newdict = dict()
    loc = "config.xlsx"
    wb = xlrd.open_workbook(loc)
    sheet = wb.sheet_by_name(sheetname)
    for i in range(sheet.nrows):
        newdict[sheet.cell_value(i, 0)] = sheet.cell_value(i, 1)
    print(newdict)
    return newdict


def readmessage(messagepath):
    with open(messagepath, 'r+') as fobj:
        message = fobj.read()

    print(message)
    return message


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--action',
        help='pubsub' ' : Create Pub/SUb '
             'publish' ' : Publish message '
             'dataflow' ' :create job with pipeline '
             'bigquery' ' :create bigquery table '
             'storage'  ' :create storage bucket for temp folder'
        )

    known_args, pipeline_args = parser.parse_known_args()
    print(known_args, '\n', pipeline_args)

    if known_args.action == 'pubsub':
        config = readconfig('PubSub')
        checktopicavailable(project_id=config['ProjectID'], topicid=config['TopicName'])
        checksubscriptionavailable(project_id=config['ProjectID'], subscription_name=config['SubscritonName'],
                                   topicname=config['TopicName'])
    elif known_args.action == 'publish':
        config = readconfig('Publish')
        message = readmessage(config['MessagePath'])
        publishmessage(config['ProjectID'], config['TopicName'], message)

    elif known_args.action == 'bigquery':
        config = readconfig('Bigquery')
        create_dataset(config['Project_ID'],config['Dataset'],config['location'], config['Table'])

    elif known_args.action == 'storage':
        config = readconfig('Storage')
        check_bucket_available(config['Bucket'])

    elif known_args.action == 'dataflow':
        # from dfPipe import startdataflow
        config = readconfig('Dataflow')
        l = ['--project_id', config['Project_ID'],
             '--job_id', config['job_id'],
             '--region', config['region'],
             '--output_table', config['output_table'],
             '--input_subscription', config['input_subscription'],
             '--temp_folder', config['temp_folder'],
             '--window_interval', str(int(config['window_interval']))
             ]
        print(l)

        checktopicavailable(project_id=config['Project_ID'], topicid=config['TopicName'])
        checksubscriptionavailable(project_id=config['Project_ID'],
                                   subscription_name=str(config['input_subscription']).split('/')[-1],
                                   topicname=config['TopicName'])
        check_bucket_available(str(config['temp_folder']).split('/')[-2])

        outlist = str(config['output_table']).split('.')
        dataset = outlist[-2].split(':')[-1]
        table = outlist[-1]
        create_dataset(config['Project_ID'],dataset , config['location'],table)

        startdataflow(l)
