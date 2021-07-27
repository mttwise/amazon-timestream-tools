#! /usr/bin/python3

'''This module is used to import data from alldata_skab.csv using a single
thread with a batch size of 100 into Timestream.'''

# Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.


from datetime import datetime, timedelta
import csv
import boto3
from botocore.config import Config


class Demo():
    '''This class is responsible for processing the alldata_skab.csv and
    importing it to Timestream'''
    def __init__(self):
        ''' '''
        self.DATABASE_NAME = "demo"
        self.TABLE_NAME = "Ingestion_Demo_100"
        self.records = []

    def upload_record(self, record_s):
        '''This function takes in a record and uploads the record to Timestream.
        Note the Common attribute setting for the Sensor Value'''
        try:
            result = client.write_records(DatabaseName=self.DATABASE_NAME,
                                                TableName=self.TABLE_NAME,
                                                Records=record_s,
                                                CommonAttributes={"Dimensions":
                                                    [{"Name":"Sensor","Value":"1"}]})
        except client.exceptions.RejectedRecordsException as err:
            print("RejectedRecords: ", err)
            for rr in err.response["RejectedRecords"]:
                print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
                print("Other records were written successfully. ")
        except Exception as e:
            print(e)

    def create_record(self, measurement):
        '''This function combines the measure with the dimensions to
        upload into Timestream'''
        dimensions = []
        measurement['Dimensions'] = dimensions
        self.records.append(measurement)
        if len(self.records) == 100:
            self.upload_record(self.records)
            self.records = []

    def create_measurement(self, metric_name, metric, time):
        '''This function formats the measurement into the style needed to import
        into Timestream'''
        result = {}
        result["MeasureName"] = metric_name
        result["MeasureValue"] = metric
        result["MeasureValueType"] = "DOUBLE"
        result["Time"] = str(time)
        result["TimeUnit"] = "MILLISECONDS"
        return result

    def start(self):
        '''This functions starts the process, reads the csv, and generates timestamps
        starting 20 hours in the past.'''
        start_time = datetime.now()
        print(start_time)
        origin_time = int((start_time - timedelta(hours=20, minutes=00)).timestamp()*1000)

        f = open('alldata_skab.csv', 'r')

        with f:
            reader = csv.DictReader(f)
            increment = 0
            for row in reader:
                increment += 1000
                accelerometer_1_rms = self.create_measurement("Accelerometer1RMS",
                                                                row['Accelerometer1RMS'],
                                                                origin_time + increment)
                self.create_record(accelerometer_1_rms)

                accelerometer_2_rms = self.create_measurement("Accelerometer2RMS",
                                                                row['Accelerometer2RMS'],
                                                                origin_time + increment)
                self.create_record(accelerometer_2_rms)

                current = self.create_measurement("Current",
                                                    row['Current'],
                                                    origin_time + increment)
                self.create_record(current)

                pressure = self.create_measurement("Pressure",
                                                    row['Pressure'],
                                                    origin_time + increment)
                self.create_record(pressure)

                temperature = self.create_measurement("Temperature",
                                                        row['Temperature'],
                                                        origin_time + increment)
                self.create_record(temperature)

                thermocouple = self.create_measurement("Thermocouple",
                                                        row['Thermocouple'],
                                                        origin_time + increment)
                self.create_record(thermocouple)

                volume_flow_rate_rms = self.create_measurement("Volume Flow RateRMS",
                                                                row['Volume Flow RateRMS'],
                                                                origin_time + increment)
                self.create_record(volume_flow_rate_rms)

        end_time = datetime.now()

        processing_time = end_time - start_time
        print(processing_time)

    def authenticate(self):
        '''This function authenticates to AWS using boto3'''
        session = boto3.Session()
        write_client = session.client('timestream-write', config=Config(read_timeout=20,
                                                                    max_pool_connections=5000,
                                                                    retries={'max_attempts': 10}))
        return write_client

if __name__ == "__main__":
    demo = Demo()
    client = demo.authenticate()
    demo.start()
