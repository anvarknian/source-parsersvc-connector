#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import io
import csv

import zipfile
import json
import tempfile
import traceback
import urllib
from typing import Iterable
import os
import backoff
import google
import numpy as np
import pandas as pd
import smart_open
from airbyte_cdk.entrypoint import logger
from airbyte_cdk.models import AirbyteStream, SyncMode
from genson import SchemaBuilder

from yaml import safe_load
import requests
from .constants import PARSERSVC_URL
from .utils import backoff_handler


class ConfigurationError(Exception):
    """Client mis-configured"""


class PermissionsError(Exception):
    """User don't have enough permissions"""


class URLFile:
    def __init__(self,
                 bearer_token: str,
                 topic: str,
                 format: str,
                 binary=None, encoding=None):
        self._file = None
        self.args = {
            "mode": "rb" if binary else "r",
            "encoding": encoding,
        }
        self.bearer_token = bearer_token
        self.topic = topic
        self.format = format

    def __enter__(self):
        return self._file

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def full_url(self):
        return f"{PARSERSVC_URL}/{self.topic}/?format={self.format}"

    def close(self):
        if self._file:
            self._file.close()
            self._file = None

    # def open(self):
    #     self.close()
    #     try:
    #         self._file = self._open()
    #     except google.api_core.exceptions.NotFound as err:
    #         raise FileNotFoundError(f"{PARSERSVC_URL}/{self.topic}/?format={self.format}") from err
    #     return self

    def _open(self):
        airbyte_version = os.environ.get("AIRBYTE_VERSION", "0.0")
        transport_params = {"headers":
            {
                'Authorization': 'Bearer {}'.format(self.bearer_token)
            }
        }
        logger.info(f"TransportParams: {transport_params}")
        with smart_open.open(self.full_url,
                             'rb',
                             transport_params=transport_params,
                             **self.args) as file:
            # Create a ZipFile object from the file contents
            zipfile_obj = zipfile.ZipFile(io.BytesIO(file.read()))

            # Find the CSV file in the ZipFile object
            csv_filename = None
            for filename in zipfile_obj.namelist():
                if filename.endswith('.csv'):
                    csv_filename = filename
                    break

            # Check if a CSV file was found in the ZipFile object
            if csv_filename is not None:

                # Open the CSV file in the ZipFile object
                csv_file = zipfile_obj.open(csv_filename)

                # Read the CSV data into a list of dictionaries
                c = csv.DictReader(io.TextIOWrapper(csv_file))
                csv_data = list(c)
                print(csv_data)

                # Do something with the CSV data
                return  c
            else:
                logger.error(f"No CSV file found in the zip file")
                raise Exception('No CSV file found in the zip file')
    def _open(self):
        headers = {"Authorization": 'Bearer {}'.format(self.bearer_token)}
        logger.info(f"headers: {headers}")
        try:
            response = requests.get(self.full_url, headers=headers, stream=True)
            response.raise_for_status()
            filename = "./{}.{}".format(self.topic, self.format)
            if os.path.exists(filename):
                os.remove(filename)
            with open(filename, 'wb') as f:
                f.write(response.content)
            return smart_open.open(filename, **self.args)

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {self.topic}.{self.format} dump: {e}")
            raise e


class Client:
    """Class that manages reading and parsing data from streams"""

    CSV_CHUNK_SIZE = 10_000
    reader_class = URLFile
    binary_formats = {"excel", "excel_binary", "feather", "parquet", "orc", "pickle"}

    def __init__(self,
                 bearer_token: str,
                 topic: str,
                 format: str = None,
                 reader_options: dict = None):
        self._bearer_token = bearer_token
        self._topic = topic
        self._reader_format = format or "csv"
        self._reader_options = reader_options or {}
        self.binary_source = self._reader_format in self.binary_formats
        self.encoding = self._reader_options.get("encoding")

    @property
    def stream_name(self) -> str:
        return f"file_{self._topic}.{self._reader_format}"

    def load_nested_json_schema(self, fp) -> dict:
        # Use Genson Library to take JSON objects and generate schemas that describe them,
        builder = SchemaBuilder()
        if self._reader_format == "jsonl":
            for o in self.read():
                builder.add_object(o)
        else:
            builder.add_object(json.load(fp))

        result = builder.to_schema()
        if "items" in result:
            # this means we have a json list e.g. [{...}, {...}]
            # but need to emit schema of an inside dict
            result = result["items"]
        result["$schema"] = "http://json-schema.org/draft-07/schema#"
        return result

    def load_nested_json(self, fp) -> list:
        if self._reader_format == "jsonl":
            result = []
            line = fp.readline()
            while line:
                result.append(json.loads(line))
                line = fp.readline()
        else:
            result = json.load(fp)
            if not isinstance(result, list):
                result = [result]
        return result

    def load_yaml(self, fp):
        if self._reader_format == "yaml":
            return pd.DataFrame(safe_load(fp))

    def load_dataframes(self, fp, skip_data=False) -> Iterable:
        """load and return the appropriate pandas dataframe.

        :param fp: file-like object to read from
        :param skip_data: limit reading data
        :return: a list of dataframe loaded from files described in the configuration
        """
        readers = {
            "csv": pd.read_csv,
            "flat_json": pd.read_json,
            "html": pd.read_html,
            "excel": pd.read_excel,
            "excel_binary": pd.read_excel,
            "feather": pd.read_feather,
            "parquet": pd.read_parquet,
            "orc": pd.read_orc,
            "pickle": pd.read_pickle,
        }

        try:
            reader = readers[self._reader_format]
        except KeyError as err:
            error_msg = f"Reader {self._reader_format} is not supported\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg) from err

        reader_options = {**self._reader_options}
        try:
            if self._reader_format == "csv":
                reader_options["chunksize"] = self.CSV_CHUNK_SIZE
                if skip_data:
                    reader_options["nrows"] = 0
                    reader_options["index_col"] = 0
                yield from reader(fp, **reader_options)
            elif self._reader_options == "excel_binary":
                reader_options["engine"] = "pyxlsb"
                yield from reader(fp, **reader_options)
            else:
                yield reader(fp, **reader_options)
        except UnicodeDecodeError as err:
            error_msg = f"File {fp} can't be parsed with reader of chosen type ({self._reader_format})\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg) from err

    @staticmethod
    def dtype_to_json_type(current_type: str, dtype) -> str:
        """Convert Pandas Dataframe types to Airbyte Types.

        :param current_type: str - one of the following types based on previous dataframes
        :param dtype: Pandas Dataframe type
        :return: Corresponding Airbyte Type
        """
        number_types = ("int64", "float64")
        if current_type == "string":
            # previous column values was of the string type, no sense to look further
            return current_type
        if dtype == object:
            return "string"
        if dtype in number_types and (not current_type or current_type in number_types):
            return "number"
        if dtype == "bool" and (not current_type or current_type == "boolean"):
            return "boolean"
        return "string"

    @property
    def reader(self) -> reader_class:
        return self.reader_class(
            binary=self.binary_source,
            encoding=self.encoding,
            bearer_token=self._bearer_token,
            format=self._reader_format,
            topic=self._topic)

    @backoff.on_exception(backoff.expo, ConnectionResetError, on_backoff=backoff_handler, max_tries=5, max_time=60)
    def read(self, fields: Iterable = None) -> Iterable[dict]:
        """Read data from the stream"""
        with self.reader.open() as fp:
            try:
                if self._reader_format in ["json", "jsonl"]:
                    yield from self.load_nested_json(fp)
                elif self._reader_format == "yaml":
                    fields = set(fields) if fields else None
                    df = self.load_yaml(fp)
                    columns = fields.intersection(set(df.columns)) if fields else df.columns
                    df = df.where(pd.notnull(df), None)
                    yield from df[columns].to_dict(orient="records")
                else:
                    fields = set(fields) if fields else None
                    if self.binary_source:
                        fp = self._cache_stream(fp)
                    for df in self.load_dataframes(fp):
                        columns = fields.intersection(set(df.columns)) if fields else df.columns
                        df.replace({np.nan: None}, inplace=True)
                        yield from df[list(columns)].to_dict(orient="records")
            except ConnectionResetError:
                logger.info(
                    f"Catched `connection reset error - 104`, stream: {self.stream_name} ({self.reader.full_url})")
                raise ConnectionResetError

    def _cache_stream(self, fp):
        """cache stream to file"""
        fp_tmp = tempfile.TemporaryFile(mode="w+b")
        fp_tmp.write(fp.read())
        fp_tmp.seek(0)
        fp.close()
        return fp_tmp

    def _stream_properties(self, fp):
        if self._reader_format == "yaml":
            df_list = [self.load_yaml(fp)]
        else:
            if self.binary_source:
                fp = self._cache_stream(fp)
            df_list = self.load_dataframes(fp, skip_data=False)
        fields = {}
        for df in df_list:
            for col in df.columns:
                # if data type of the same column differs in dataframes, we choose the broadest one
                prev_frame_column_type = fields.get(col)
                fields[col] = self.dtype_to_json_type(prev_frame_column_type, df[col].dtype)
        return {field: {"type": [fields[field], "null"]} for field in fields}

    @property
    def streams(self) -> Iterable:
        """Discovers available streams"""
        # TODO handle discovery of directories of multiple files instead
        with self.reader.open() as fp:
            if self._reader_format in ["json", "jsonl"]:
                json_schema = self.load_nested_json_schema(fp)
            else:
                json_schema = {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": self._stream_properties(fp),
                }
        yield AirbyteStream(name=self.stream_name, json_schema=json_schema,
                            supported_sync_modes=[SyncMode.full_refresh])
