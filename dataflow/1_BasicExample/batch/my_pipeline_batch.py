import argparse
import time
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import DataflowRunner, DirectRunner

# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Avro into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--stagingLocation', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--tempLocation', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions()
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.stagingLocation
    options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('my-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    # Static input and output
    input = 'gs://{0}/characters.avro'.format(opts.project)
    output = '{0}:logs.logs'.format(opts.project)

    # Table schema for BigQuery
    table_schema = {
    "fields": [
        {
            "name": "id",
            "type": "INTEGER"
        },
        {
            "name": "name",
            "type": "STRING"
        },
        {
            "name": "status",
            "type": "STRING"
        },
        {
            "name": "species",
            "type": "STRING"
        },
        {
            "name": "type",
            "type": "STRING"
        },
        {
            "name": "gender",
            "type": "STRING"
        },
        {
            "name": "origin",
            "type": "RECORD",
            "fields": [
                {
                    "name": "name",
                    "type": "STRING"
                },
                {
                    "name": "url",
                    "type": "STRING"
                }
            ]
        },
        {
            "name": "location",
            "type": "RECORD",
            "fields": [
                {
                    "name": "name",
                    "type": "STRING"
                },
                {
                    "name": "url",
                    "type": "STRING"
                }
            ]
        },
        {
            "name": "image",
            "type": "STRING"
        },
        {
            "name": "episode",
            "type": "STRING",
            "mode": "REPEATED"
        },
        {
            "name": "url",
            "type": "STRING"
        },
        {
            "name": "created",
            "type": "TIMESTAMP"
        }
    ]
}

    # Create the pipeline
    p = beam.Pipeline(options=options)

    '''

    Steps:
    1) Read something
    2) Transform something
    3) Write something

    '''

    (p
        | 'ReadFromGCS' >> beam.io.ReadFromAvro(input)
        | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            output,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()

if __name__ == '__main__':
  run()