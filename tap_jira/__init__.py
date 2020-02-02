#!/usr/bin/env python3
import argparse
import json
import os

import boto3
import singer
from google.api_core.exceptions import NotFound
from google.cloud import storage
from singer import metadata
from singer import utils
from singer.catalog import Catalog, CatalogEntry, Schema

from . import streams as streams_
from .context import Context
from .http import Client

LOGGER = singer.get_logger()
REQUIRED_CONFIG_KEYS_CLOUD = ["start_date",
                              "user_agent",
                              "cloud_id",
                              "access_token",
                              "refresh_token",
                              "oauth_client_id",
                              "oauth_client_secret",
                              "gcs_state_bucket",
                              "gcs_state_blob"]
REQUIRED_CONFIG_KEYS_HOSTED = ["start_date",
                               "username",
                               "password",
                               "base_url",
                               "user_agent"]


def parse_args(required_config_keys):
    '''Parse standard command-line args.

    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:

    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -p,--properties Properties file: DEPRECATED, please use --catalog instead
    --catalog       Catalog file

    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    parser.add_argument(
        '-s', '--state',
        help='State file')

    parser.add_argument(
        '-p', '--properties',
        help='Property selections: DEPRECATED, Please use --catalog instead')

    parser.add_argument(
        '--catalog',
        help='Catalog file')

    parser.add_argument(
        '-d', '--discover',
        action='store_true',
        help='Do schema discovery')

    parser.add_argument(
        '-e', '--env',
        help='Environment')

    args = parser.parse_args()
    if args.config:
        args.config = utils.load_json(args.config)
    if args.state:
        args.state = utils.load_json(args.state)
    else:
        args.state = {}
    if args.properties:
        args.properties = utils.load_json(args.properties)
    if args.catalog:
        args.catalog = Catalog.load(args.catalog)

    utils.check_config(args.config, required_config_keys)

    return args


def get_param(key):
    ssm = boto3.client('ssm', region_name='eu-west-1')
    parameter = ssm.get_parameter(Name=key, WithDecryption=True)
    return parameter['Parameter']['Value']


def get_with_aws(value, env):
    if value.startswith("aws"):
        return get_param(f'/{env}/{value}')


def get_args():
    unchecked_args = utils.parse_args([])
    if 'username' in unchecked_args.config.keys():
        return utils.parse_args(REQUIRED_CONFIG_KEYS_HOSTED)

    return utils.parse_args(REQUIRED_CONFIG_KEYS_CLOUD)


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    refs = schema.pop("definitions", {})
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema


def discover():
    catalog = Catalog([])
    for stream in streams_.ALL_STREAMS:
        schema = Schema.from_dict(load_schema(stream.tap_stream_id))

        mdata = generate_metadata(stream, schema)

        catalog.streams.append(CatalogEntry(
            stream=stream.tap_stream_id,
            tap_stream_id=stream.tap_stream_id,
            key_properties=stream.pk_fields,
            schema=schema,
            metadata=mdata))
    return catalog


def generate_metadata(stream, schema):
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), 'table-key-properties', stream.pk_fields)

    for field_name in schema.properties.keys():
        if field_name in stream.pk_fields:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)


def output_schema(stream):
    schema = load_schema(stream.tap_stream_id)
    singer.write_schema(stream.tap_stream_id, schema, stream.pk_fields)


def sync():
    streams_.validate_dependencies()

    # two loops through streams are necessary so that the schema is output
    # BEFORE syncing any streams. Otherwise, the first stream might generate
    # data for the second stream, but the second stream hasn't output its
    # schema yet
    for stream in streams_.ALL_STREAMS:
        output_schema(stream)

    for stream in streams_.ALL_STREAMS:
        if not Context.is_selected(stream.tap_stream_id):
            continue

        # indirect_stream indicates the data for the stream comes from some
        # other stream, so we don't sync it directly.
        if stream.indirect_stream:
            continue
        Context.state["currently_syncing"] = stream.tap_stream_id
        singer.write_state(Context.state)
        stream.sync()
    Context.state["currently_syncing"] = None
    singer.write_state(Context.state)


def main_impl():
    args = get_args()

    # Setup Context
    catalog = Catalog.from_dict(args.properties) \
        if args.properties else discover()
    Context.config = {k: get_with_aws(v, args.env) for k, v in args.config.items()}

    bucket_name = Context.config.get("gcs_state_bucket")
    blob_name = Context.config.get("gcs_state_blob")

    if blob_name and bucket_name:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        try:
            Context.state = json.loads(blob.download_as_string())
        except NotFound:
            Context.state = args.state
    else:
        Context.state = args.state

    Context.catalog = catalog

    Context.client = Client(Context.config)

    try:
        if args.discover:
            discover().dump()
            print()
        else:
            sync()
    finally:
        if Context.client and Context.client.login_timer:
            Context.client.login_timer.cancel()


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == "__main__":
    main()
