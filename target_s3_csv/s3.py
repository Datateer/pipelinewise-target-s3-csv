#!/usr/bin/env python3
import csv
from datetime import datetime
import json
import io
import os
import sys
import tempfile
import time

import boto3

import gzip
import os
import shutil
import backoff
import boto3
import singer

from typing import Optional, Tuple, List, Dict, Iterator
from botocore.client import BaseClient
from botocore.client import Config
from botocore.exceptions import ClientError
import humanize

LOGGER = singer.get_logger()


def retry_pattern():
    return backoff.on_exception(backoff.expo,
                                ClientError,
                                max_tries=1,
                                on_backoff=log_backoff_attempt,
                                factor=10)


def log_backoff_attempt(details):
    LOGGER.info("Error detected communicating with Amazon, triggering backoff: %d try", details.get("tries"))

class S3MultipartUploader(object):
    # AWS throws EntityTooSmall error for parts smaller than 5 MB
    PART_MINIMUM_SIZE = int(5e6)

    def __init__(self,
                 config, # the singer config object
                 stream_name,  # the name of the singer stream
                 s3_key,
                 include_header_row=True,
                 profile_name=None,  # the AWS profile name
                 region_name='us-east-1',  # the AWS region
                 verbose=False):
        self.bucket = config['s3_bucket']
        self.key = s3_key
        self.csv_delimiter = config.get('delimiter', ',')
        self.csv_quotechar = config.get('quotechar', '"')
        self.include_header_row = include_header_row # todo: consider including in config, or remove
        self.part_records = int(config.get('upload_batch_record_count', 100000))
        session = boto3.Session(aws_access_key_id=config.get('aws_access_key_id') or os.getenv('AWS_ACCESS_KEY_ID'),aws_secret_access_key=config.get('aws_secret_access_key') or os.getenv('AWS_SECRET_ACCESS_KEY'))
        self.s3 = session.client('s3', config=Config(signature_version='s3v4'))
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
        """Returns tuple of bool: did_flush_records_to_target, int: record_count"""
        self.records.append(record)
        self.total_records += 1
        # LOGGER.info(f'added record {self.total_records}')
        if len(self.records) >= self.part_records:
            record_count = len(self.records)
            self.upload()
            return True, record_count
        else:
            return False, None


    @retry_pattern()
    def upload(self, state=None):
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
    def complete(self):
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
# =======

# def upload_files(filenames: Iterator[Dict],
#                  s3_client: BaseClient,
#                  s3_bucket: str,
#                  compression: Optional[str],
#                  encryption_type: Optional[str],
#                  encryption_key: Optional[str]):
#     """
#     Uploads given local files to s3
#     Compress if necessary
#     """
#     for file in filenames:
#         filename, target_key = file['filename'], file['target_key']
#         compressed_file = None

#         if compression is not None and compression.lower() != "none":
#             if compression == "gzip":
#                 compressed_file = f"{filename}.gz"
#                 target_key = f'{target_key}.gz'

#                 with open(filename, 'rb') as f_in:
#                     with gzip.open(compressed_file, 'wb') as f_out:
#                         LOGGER.info(f"Compressing file as '%s'", compressed_file)
#                         shutil.copyfileobj(f_in, f_out)

#             else:
#                 raise NotImplementedError(
#                     "Compression type '{}' is not supported. Expected: 'none' or 'gzip'".format(compression)
#                 )

#         upload_file(compressed_file or filename,
#                     s3_client,
#                     s3_bucket,
#                     target_key,
#                     encryption_type=encryption_type,
#                     encryption_key=encryption_key
#                     )

#         # Remove the local file(s)
#         if os.path.exists(filename):
#             os.remove(filename)
#             if compressed_file:
#                 os.remove(compressed_file)

