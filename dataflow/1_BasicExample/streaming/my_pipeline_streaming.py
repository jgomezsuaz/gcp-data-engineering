import argparse
import time
import logging
import json
import typing
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.runners import DataflowRunner, DirectRunner

# ### functions and classes

class CommonLog(typing.NamedTuple):
    sensor: str
    timestamp: int
    co: float
    humidity: float
    light: bool
    lpg: float
    motion: bool
    smoke: float
    temp: float

beam.coders.registry.register_coder(CommonLog, beam.coders.RowCoder)

def parse_json(element):
    row = json.loads(element.decode('utf-8'))
    return CommonLog(**row)

def add_processing_timestamp(element):
    row = element._asdict()
    row['event_timestamp'] = row.pop('timestamp')
    row['processing_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return row

class GetTimestampFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime().strftime("%Y-%m-%dT%H:%M:%S")
        avg, count = element
        output = {'avg': avg, 'timestamp': window_start, 'count': count}
        yield output
        
class AverageAndCountCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return (0.0, 0)  # (suma, conteo)

    def add_input(self, accumulator, element):
        sum_temp, count = accumulator
        return sum_temp + element, count + 1

    def merge_accumulators(self, accumulators):
        sum_temp = sum(count_temp[0] for count_temp in accumulators)
        count = sum(count_temp[1] for count_temp in accumulators)
        return sum_temp, count

    def extract_output(self, accumulator):
        sum_temp, count = accumulator
        return sum_temp / count if count > 0 else 0.0, count

# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json from Pub/Sub into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--staging_location', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--temp_location', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--input_topic', required=True, help='Input Pub/Sub Topic')
    parser.add_argument('--agg_table_name', required=True, help='BigQuery table name for aggregate results')
    parser.add_argument('--raw_table_name', required=True, help='BigQuery table name for raw inputs')
    parser.add_argument('--window_duration', required=True, help='Window duration')

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(save_main_session=True, streaming=True)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.staging_location
    options.view_as(GoogleCloudOptions).temp_location = opts.temp_location
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('streaming-minute-traffic-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    input_topic = opts.input_topic
    raw_table_name = opts.raw_table_name
    agg_table_name = opts.agg_table_name
    window_duration = opts.window_duration

    # Table schema for BigQuery
    agg_table_schema = {
        "fields": [
  {
    "name": "avg",
    "type": "FLOAT"
  },
  {
    "name": "timestamp",
    "type": "TIMESTAMP"
  },
  {
    "name": "count",
    "type": "INTEGER"
  }
]

    }

    raw_table_schema = {
        "fields": [
  {
    "name": "co",
    "type": "FLOAT"
  },
  {
    "name": "humidity",
    "type": "FLOAT"
  },
  {
    "name": "light",
    "type": "BOOLEAN"
  },
  {
    "name": "lpg",
    "type": "FLOAT"
  },
  {
    "name": "motion",
    "type": "BOOLEAN"
  },
  {
    "name": "smoke",
    "type": "FLOAT"
  },
  {
    "name": "temp",
    "type": "FLOAT"
  },
  {
    "name": "timestamp",
    "type": "TIMESTAMP"
  },
  {
    "name": "sensor",
    "type": "STRING"
  }
]

    }

    # Create the pipeline
    p = beam.Pipeline(options=options)



    parsed_msgs = (p | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(input_topic)
                     | 'ParseJson' >> beam.Map(parse_json).with_output_types(CommonLog))

    (parsed_msgs
        | 'FilterPerTemp' >> beam.Filter(lambda element: element.temp > 26.0)
        | 'ConvertToJSON' >> beam.Map(lambda data: {
        "sensor": data.sensor,
        "timestamp": data.timestamp,
        "co": data.co,
        "humidity": data.humidity,
        "light": data.light,
        "lpg": data.lpg,
        "motion": data.motion,
        "smoke": data.smoke,
        "temp": data.temp
        })
        | 'WriteRawToBQ' >> beam.io.WriteToBigQuery(
            raw_table_name,
            schema=raw_table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

    (parsed_msgs
        | "WindowByMinute" >> beam.WindowInto(beam.window.FixedWindows(60))
        | 'Get temperatures' >> beam.Map(lambda data: data.temp)
        | 'CalculateAverage' >> beam.CombineGlobally(AverageAndCountCombineFn()).without_defaults()
        | "AddWindowTimestamp" >> beam.ParDo(GetTimestampFn())
        | 'WriteAggToBQ' >> beam.io.WriteToBigQuery(
            agg_table_name,
            schema=agg_table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()

if __name__ == '__main__':
  run()