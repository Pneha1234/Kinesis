import json
import os
from dotenv import load_dotenv
import logger
from botocore.exceptions import ClientError
import boto3
import datetime


class KinesisStream:

    def __init__(self, kinesis_client):
        self.kinesis_client = kinesis_client
        self.name = 'demo_data_stream'
        self.details = None
        self.stream_exists_waiter = kinesis_client.get_waiter('stream_exists')

    def create(self, name, wait_until_exists=True):
        """
        Creates a stream.

        :param name: The name of the stream.
        :param wait_until_exists: When True, waits until the service reports that
                                  the stream exists, then queries for its metadata.
        """
        try:
            self.kinesis_client.create_stream(StreamName=name, ShardCount=1)
            self.name = name
            logger.info("Created stream %s.", name)
            if wait_until_exists:
                print("Waiting until exists.")
                self.stream_exists_waiter.wait(StreamName=name)
                self.describe(name)
        except ClientError:
            print("Couldn't create stream %s.", name)
            raise

    def describe(self, name):
        """
        Gets metadata about a stream.

        :param name: The name of the stream.
        :return: Metadata about the stream.
        """
        try:
            response = self.kinesis_client.describe_stream(StreamName=name)
            self.name = name
            self.details = response['StreamDescription']
            print("Got stream %s.", name)
        except ClientError:
            print("Couldn't get %s.", name)
            raise
        else:
            return self.details

    def get_records(self, max_records, date_after):
        print("hello")
        """
        Gets records from the stream. This function is a generator that first gets
        a shard iterator for the stream, then uses the shard iterator to get records
        in batches from the stream. Each batch of records is yielded back to the
        caller until the specified maximum number of records has been retrieved.

        :param max_records: The maximum number of records to retrieve.
        :return: Yields the current batch of retrieved records.
        """
        try:
            response = self.kinesis_client.get_shard_iterator(
                StreamName=self.name, ShardId=self.details['Shards'][0]['ShardId'],
                ShardIteratorType='AT_TIMESTAMP',
                Timestamp=date_after)
            shard_iter = response['ShardIterator']
            record_count = 0
            while record_count < max_records:
                response = self.kinesis_client.get_records(
                    ShardIterator=shard_iter, Limit=10)
                shard_iter = response['NextShardIterator']
                records = response['Records']
                print("Got %s records.", len(records))
                record_count += len(records)
                yield records
        except ClientError:
            print("Couldn't get records from stream %s.", self.name)
            raise

    def put_record(self, data, partition_key):
        """
        Puts data into the stream. The data is formatted as JSON before it is passed
        to the stream.

        :param data: The data to put in the stream.
        :param partition_key: The partition key to use for the data.
        :return: Metadata about the record, including its shard ID and sequence number.
        """
        try:
            response = self.kinesis_client.put_record(
                StreamName=self.name,
                Data=json.dumps(data),
                PartitionKey=partition_key)
            print("Put record in stream %s.", self.name)
        except ClientError:
            print("Couldn't put record in stream %s.", self.name)
            raise
        else:
            return response


def get_aws_kinesis_client():
    load_dotenv('dev.env')
    session = boto3.client(
        'kinesis',
        aws_access_key_id=os.environ['AWS_SERVER_PUBLIC_KEY'],
        aws_secret_access_key=os.environ['AWS_SERVER_SECRET_KEY'],
        region_name='us-east-1'
    )

    return session


kinesis_client = get_aws_kinesis_client()
stream = KinesisStream(kinesis_client)
# stream.create("demo_data_stream")
details = stream.describe(name='demo_data_stream')
response = stream.put_record(data={"test1":"testVal2", "c":"d"}, partition_key='123456')
records = stream.get_records(max_records=10, date_after=datetime.datetime(2023, 6, 19))
records = list(records)
records = [[j['Data'].decode("utf-8")  for j in i] for i in records if len(i) > 0 ]
records = [item for sublist in records for item in sublist]

with open('failing_records.json', 'w') as outfile:
    json.dump(records, outfile, default=str)