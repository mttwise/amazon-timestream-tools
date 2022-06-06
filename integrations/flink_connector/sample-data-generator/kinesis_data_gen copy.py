import argparse
import json
import random
import signal
import string
import sys
import time
from collections import namedtuple
import distutils

import boto3
import numpy as np

measureValue = 'value'
measurementId = 'measurementId'
measureName = 'measureName'
measureQuality = 'quality'

measuresForMetrics = [measureValue]

selectionProbabilities = [0.2, 0.7, 0.01, 0.07, 0.01, 0.01]

DimensionsMetric = namedtuple('DimensionsMetric',
                              'measurementId measureName measureQuality')

utilizationRand = random.Random(12345)

class MeasureValue:
    def __init__(self, name, value, type):
        self.name = name
        if type == 'DOUBLE':
            self.value = round(value, 2)
        else:
            self.value = value
        self.type = type


def generateRandomAlphaNumericString(length=5):
    rand = random.Random(12345)
    x = ''.join(rand.choices(string.ascii_letters + string.digits, k=length))
    print(x)
    return x

def createRandomMetrics(timestamp, time_unit, dimensions):

    measure_value = 100.0 * random.random()

    return [create_record('data', measure_value, timestamp, dimensions)]

def current_milli_time():
    return round(time.time() * 1000)

def create_record(object_type, measure_value, timestamp, dimensions):
    record = dict(dimensions)

    record['value'] = measure_value
    record['time'] = timestamp
    record['@type'] = object_type
    return record

def generateDimensions(scaleFactor):
    instancePrefix = generateRandomAlphaNumericString(8)
    dimensionsMetrics = list()


    for i in range(0, 1000, 1):

        measurementId = f'testMeasure_{i}'
        measureName = f'sensor{i}'
        measureQuality = 'GOOD'

        metric = DimensionsMetric(measurementId, measureName, measureQuality)
        dimensionsMetrics.append(metric)

    return dimensionsMetrics
    


def send_records_to_kinesis(all_dimensions, kinesis_client, stream_name, sleep_time, percent_late, late_time):
    while True:
        if percent_late > 0:
            value = random.random()*100
            if (value >= percent_late):
                print('Generating On-Time Records.')
                local_timestamp = int(current_milli_time())
            else:
                print('Generating Late Records.')
                local_timestamp = (int(current_milli_time()) - late_time)
        else:
            local_timestamp = int(current_milli_time())

        records = []
        for series_id, dimensions in enumerate(all_dimensions):
            dimension_dict = dimensions._asdict()
            if isinstance(dimensions, DimensionsMetric):
                metrics = createRandomMetrics(local_timestamp, 'MILLISECONDS', dimension_dict)            

           
            for metric in metrics:
                # print(metric)
                data = json.dumps(metric)
                records.append({'Data': bytes(data, 'utf-8'), 'PartitionKey': dimension_dict['measurementId']})
        
        split_arr = divide_chunks(records, 500)

        for record_set in split_arr:
            kinesis_client.put_records(StreamName=stream_name, Records=record_set)
        print('Wrote metric record to Kinesis Stream \'{}\''.format(stream_name))
        
        if sleep_time > 0:
            time.sleep(float(sleep_time))

def divide_chunks(l, n):
      
    # looping till length l
    for i in range(0, len(l), n): 
        yield l[i:i + n]

def main(args):

    print(args)
    generate_metrics = args.generate_metrics

    if not generate_metrics:
        print('Exiting: generate_metrics is false, so no data will be generated')
        sys.exit(0)

    host_scale = args.hostScale  # scale factor for the hosts.

    dimensions_measures = generateDimensions(host_scale)

    if generate_metrics:
        print('Dimensions for metrics: {}'.format(len(dimensions_measures)))

    def signal_handler(sig, frame):
        print('Exiting Application')
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    stream_name = args.stream
    region_name = args.region
    kinesis_client = boto3.client('kinesis', region_name=region_name)

    sleep_time = args.sleep_time
    if sleep_time is 0:
        sleep_time = 1 # Default to 1/sec
    percent_late = args.percent_late
    late_time = args.late_time

    try:
        kinesis_client.describe_stream(StreamName=stream_name)
    except:
        print('Unable to describe Kinesis Stream \'{}\' in region {}'.format(stream_name, region_name))
        sys.exit(0)

    dimensions = dimensions_measures if generate_metrics else []
    send_records_to_kinesis(dimensions,
                            kinesis_client, stream_name, sleep_time, percent_late, late_time)

def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    if v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False

    raise argparse.ArgumentTypeError('Boolean value expected.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='timestream_kinesis_data_gen',
                                     description='DevOps Sample Data Generator for Timestream/KDA Sample Application.')

    parser.add_argument('--stream', action='store', type=str, default='TimestreamTestStream',
                        help='The name of Kinesis Stream.')
    parser.add_argument('--region', '-e', action='store', choices=['us-east-1', 'us-east-2', 'us-west-2', 'eu-west-1'],
                        default='us-east-1', help='Specify the region of the Kinesis Stream.')
    parser.add_argument('--host-scale', dest='hostScale', action='store', type=int, default=1,
                        help='The scale factor determines the number of hosts emitting events and metrics.')
    parser.add_argument('--profile', action='store', type=str, default=None, help='The AWS Config profile to use.')

    # Optional sleep timer to slow down data
    parser.add_argument('--sleep-time', action='store', type=int, default=0,
                        help='The amount of time in seconds to sleep between sending batches.')

    # Optional 'Late' arriving data parameters
    parser.add_argument('--percent-late', action='store', type=float, default=0,
                        help='The percentage of data written that is late arriving ')
    parser.add_argument('--late-time', action='store', type=int, default=0,
                        help='The amount of time in seconds late that the data arrives')

    # Optional type of data to generate
    parser.add_argument('--generate-events', action='store', type=str2bool, default=True,
                        help='Whether to generate events')
    parser.add_argument('--generate-metrics', action='store', type=str2bool, default=True,
                        help='Whether to generate metrics')

    main(parser.parse_args())
