#!/usr/bin/env python3

import argparse
import csv
import gzip
import io
import json
import os
import shutil
import sys
from datetime import datetime
import time
import tempfile
import singer

from datetime import datetime
from jsonschema import Draft7Validator, FormatChecker

from target_s3_csv import s3
from target_s3_csv import utils

logger = singer.get_logger()

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


# pylint: disable=too-many-locals,too-many-branches,too-many-statements
def process_message_stream(input_message_stream, config):
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}

    delimiter = config.get('delimiter', ',')
    quotechar = config.get('quotechar', '"')
    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    for message in iter(input_message_stream):
        try:
            o = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(message))
            raise
        message_type = o['type']
        try:
            o = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(message))
            raise
        message_type = o['type']
        if message_type == 'RECORD':
            stream_name = o['stream']

            if stream_name not in schemas:
                raise Exception("A record for stream {}"
                                "was encountered before a corresponding schema".format(stream_name))

            # Validate record
            try:
                validators[stream_name].validate(utils.float_to_decimal(o['record']))
            except Exception as ex:
                if type(ex).__name__ == "InvalidOperation":
                    logger.error("Data validation failed and cannot load to destination. \n"
                                 "'multipleOf' validations that allows long precisions are not supported"
                                 " (i.e. with 15 digits or more). Try removing 'multipleOf' methods from JSON schema.")
                    raise ex

            record_to_load = o['record']
            if config.get('add_metadata_columns'):
                record_to_load = utils.add_metadata_values_to_record(o, {})
            else:
                record_to_load = utils.remove_metadata_values_from_record(o)

            flattened_record = utils.flatten_record(record_to_load)
            yield (o['stream'], flattened_record, message_type)

        elif message_type == 'STATE':
            logger.info(f'Received state from tap: {o["value"]}')
            yield(None, o, message_type)
            # logger.info('Setting state to {}'.format(o['value']))

        elif message_type == 'SCHEMA':
            stream_name = o['stream']
            schemas[stream_name] = o['schema']

            if config.get('add_metadata_columns'):
                schemas[stream_name] = utils.add_metadata_columns_to_schema(o)

            schema = utils.float_to_decimal(o['schema'])
            validators[stream_name] = Draft7Validator(schema, format_checker=FormatChecker())
            key_properties[stream_name] = o['key_properties']
        elif message_type == 'ACTIVATE_VERSION':
            logger.debug('ACTIVATE_VERSION message')
        else:
            logger.warning("Unknown message type {} in message {}".format(o['type'], o))

    

def main():
    now = datetime.now().strftime('%Y%m%dT%H%M%S')
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}

    config_errors = utils.validate_config(config)
    if len(config_errors) > 0:
        logger.error("Invalid configuration:\n   * {}".format('\n   * '.join(config_errors)))
        sys.exit(1)

    # s3.setup_aws_client(config)

    uploaders = {}
    input_message_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    processed_message_stream = process_message_stream(input_message_stream, config)
    state_from_tap = None
    try:
        for (stream_name, record, message_type) in processed_message_stream:
            if message_type == 'STATE':
                state_from_tap = record
            elif message_type == 'RECORD':
                if stream_name not in uploaders:
                    s3_key = utils.get_target_key(stream_name, prefix=config.get('s3_key_prefix', ''), timestamp=now, naming_convention=config.get('naming_convention'))
                    uploaders[stream_name] = s3.S3MultipartUploader(config, stream_name, s3_key)
                did_flush_records_to_target, record_count = uploaders[stream_name].add_record(record)
                if did_flush_records_to_target and state_from_tap is not None:
                    emit_state(state_from_tap)
    finally:
        for u in uploaders.values():
            u.complete()
        if state_from_tap is not None:
            emit_state(state_from_tap)

    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
