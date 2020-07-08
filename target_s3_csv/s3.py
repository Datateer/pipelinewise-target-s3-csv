#!/usr/bin/env python3
import csv
from datetime import datetime
import io
import os
import sys
import tempfile
import time

import boto3
import backoff
import boto3
import singer
from botocore.client import Config
from botocore.exceptions import ClientError
import humanize

LOGGER = singer.get_logger('target_s3_csv')


def retry_pattern():
    return backoff.on_exception(backoff.expo,
                                ClientError,
                                max_tries=1,
                                on_backoff=log_backoff_attempt,
                                factor=10)


def log_backoff_attempt(details):
    LOGGER.info("Error detected communicating with Amazon, triggering backoff: %d try", details.get("tries"))

@retry_pattern()
def setup_aws_client(config):
    aws_access_key_id = config['aws_access_key_id']
    aws_secret_access_key = config['aws_secret_access_key']

    LOGGER.info("Attempting to create AWS session")
    boto3.setup_default_session(aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)

@retry_pattern()
def upload_file(filename, bucket, s3_key,
                encryption_type=None, encryption_key=None):
    s3_client = boto3.client('s3', config=Config(signature_version='s3v4'))
    # s3_key = "{}{}".format(key_prefix, os.path.basename(filename))

    if encryption_type is None or encryption_type.lower() == "none":
        # No encryption config (defaults to settings on the bucket):
        encryption_desc = ""
        encryption_args = None
    else:
        if encryption_type.lower() == "kms":
            encryption_args = {"ServerSideEncryption": "aws:kms"}
            if encryption_key:
                encryption_desc = (
                    " using KMS encryption key ID '{}'"
                    .format(encryption_key)
                )
                encryption_args["SSEKMSKeyId"] = encryption_key
            else:
                encryption_desc = " using default KMS encryption"
        else:
            raise NotImplementedError(
                "Encryption type '{}' is not supported. "
                "Expected: 'none' or 'KMS'"
                .format(encryption_type)
            )
    LOGGER.info(
        "Uploading {} to bucket {} at {}{}"
        .format(filename, bucket, s3_key, encryption_desc)
    )
    s3_client.upload_file(filename, bucket, s3_key, ExtraArgs=encryption_args)

class S3MultipartUploader(object):
    # AWS throws EntityTooSmall error for parts smaller than 5 MB
    PART_MINIMUM_SIZE = int(5e6)

    def __init__(self,
                 config, # the singer config object
                 stream_name,  # the name of the singer stream
                #  upload_batch_size=int(15e6),
                 include_header_row=True,
                 profile_name=None,  # the AWS profile name
                 # the size in number of records for how big a batch should be before uploading
                 region_name='us-east-1',  # the AWS region
                 timestamp=None,
                 verbose=False):
        self.bucket = config['s3_bucket']
        self.csv_delimiter = config.get('delimiter', ',')
        self.csv_quotechar = config.get('quotechar', '"')
        self.include_header_row = include_header_row # todo: consider including in config, or remove
        self.timestamp = timestamp or datetime.now().strftime('%Y%m%dT%H%M%S')
        self.key = f'{config.get("s3_key", "")}{stream_name}_{self.timestamp}.csv'
        self.part_records = int(config.get('upload_batch_record_count', 100000))
        self.s3 = boto3.client('s3', config=Config(signature_version='s3v4'))
        if verbose:
            boto3.set_stream_logger(name="botocore")
        mpu = self.s3.create_multipart_upload(Bucket=self.bucket, Key=self.key)
        self.mpu_id = mpu["UploadId"]
        self.parts = []
        self.records = []
        self.stream_name = stream_name
        self.total_records = 0
        self.total_bytes = 0

    @retry_pattern()
    def abort_all(self):
        mpus = self.s3.list_multipart_uploads(Bucket=self.bucket)
        aborted = []
        LOGGER.warn("Aborting", len(mpus), "uploads")
        if "Uploads" in mpus:
            for u in mpus["Uploads"]:
                upload_id = u["UploadId"]
                aborted.append(
                    self.s3.abort_multipart_upload(
                        Bucket=self.bucket, Key=self.key, UploadId=upload_id))
        return aborted

    def add_record(self, record):
        self.records.append(record)
        self.total_records += 1
        # LOGGER.info(f'added record {self.total_records}')
        if len(self.records) >= self.part_records:
            self.upload()

    @retry_pattern()
    def upload(self):
        if 0 == len(self.records):
            LOGGER.warn('Nothing to upload')
        part_number = len(self.parts) + 1

        data = self.transform_to_csv(self.records)
        self.total_bytes += sys.getsizeof(data)
        LOGGER.info(f'Uploading part {part_number} of stream {self.stream_name}; {humanize.intcomma(len(self.records))} records, {humanize.naturalsize(sys.getsizeof(data), binary=True)} (total so far: {humanize.intcomma(self.total_records)} records, {humanize.naturalsize(self.total_bytes, binary=True)})')
        start = time.time()
        part = self.s3.upload_part(
            Body=data, Bucket=self.bucket, Key=self.key, UploadId=self.mpu_id, PartNumber=part_number)
        end = time.time()
        LOGGER.info(f'Uploaded part {part_number} in {humanize.naturaldelta(end - start)}')

        self.parts.append(
            {"PartNumber": part_number, "ETag": part["ETag"]})
        self.records = [] # reset the records list now that we have uploaded the batch of records

    def transform_to_csv(self, records):
        buffer = io.StringIO() # io.TextIOWrapper(io.BytesIO(), encoding='utf-8') # io.BytesIO() # io.StringIO() # io.TextIOWrapper(io.BytesIO(), encoding='utf-8')
        writer = csv.DictWriter(buffer, delimiter=self.csv_delimiter, quotechar=self.csv_quotechar,
                            quoting=csv.QUOTE_MINIMAL, fieldnames=[*self.records[0]])
        if self.include_header_row and len(self.parts) == 0:
            writer.writeheader()
        writer.writerows(self.records)
        return buffer.getvalue().encode('utf-8')


    @retry_pattern()
    def complete(self,):
        if (self.records): # flush any records yet to be uploaded
            self.upload()

        LOGGER.info(
            f'Finished multipart S3 upload. {len(self.parts)} parts, {humanize.intcomma(self.total_records)} records, {humanize.naturalsize(self.total_bytes, binary=True)} bytes')
        result = self.s3.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.mpu_id,
            MultipartUpload={"Parts": self.parts})
        return result
