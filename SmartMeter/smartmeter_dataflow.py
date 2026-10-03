import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class FilterInvalid(beam.DoFn):
    def process(self, element):
        data =json.loads(element.decode("utf-8"))
        if (data.get("pressure") is not None and data.get("temperature") is not None):
            yield data

class ConvertMeasurements(beam.DoFn):
    def process(self, data):
        data["pressure"] = data["pressure"] /6.895
        data["temperature"] = data["temperature"] *1.8+ 32
        yield data


def to_byte(data):
    return json.dumps(data).encode("utf-8")


def run():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="Input Pub/Sub topic"
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Output Pub/Sub topic"
    )

    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options =PipelineOptions(
        pipeline_args,
        streaming=True,
        save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "read from PubSub"
            >> beam.io.ReadFromPubSub(topic=known_args.input)

            | "rilter"
            >> beam.ParDo(FilterInvalid())

            | "convert"
            >> beam.ParDo(ConvertMeasurements())
            | "To byte"

            >> beam.Map(to_byte)

            | "write to PubSub"
            >> beam.io.WriteToPubSub(topic=known_args.output)
        )

if __name__ == "__main__":
    run()
