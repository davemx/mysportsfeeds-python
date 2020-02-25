"""
Data Stores for MySportsFeeds API
"""

import csv
import json
import logging
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, IO
import boto3
from botocore.exceptions import ClientError


DEFAULT_FILE_STORE_DIRECTORY: str = "results"


class DataStore:
    """ Base Data Store """

    _initialized: bool = False

    def __init__(self):
        pass

    def _initialize_store(self):
        """ Initialize the store """
        pass

    def exists(self, league: str, season: str, feed: str, data_format: str, params: dict) -> bool:
        """ Stub method for subclasses """
        raise AssertionError("Checking for existence of data is not supported")

    def load(self, league: str, season: str, feed: str, data_format: str, params: dict) -> Any:
        """ Stub method for subclasses """
        raise AssertionError("Loading data is not supported")

    def store(self, data: Any, league: str, season: str, feed: str, data_format: str, params: dict) -> Any:
        """ Stub method for subclasses """
        raise AssertionError("Storing data is not supported")

    def resolve_filename(self, league: str, season: str, feed: str, data_format: str, params: dict) -> str:
        """  Generate the appropriate filename for a feed request """
        filename = "{feed}-{league}-{season}".format(league=league.lower(), season=season, feed=feed)

        if "gameid" in params:
            filename += "-" + params["gameid"]

        if "data" in params:
            filename += "-" + params["date"]
        elif "fordate" in params:
            filename += "-" + params["fordate"]

        filename += "." + data_format

        return filename


class FileStore(DataStore):
    """ Local Filesystem Store """

    TYPE_NAME: str = "file"
    _dir_path: Path

    def __init__(self, directory: str):
        super().__init__()
        self._dir_path = Path(directory if directory is not None else DEFAULT_FILE_STORE_DIRECTORY)

    def _initialize_store(self):
        if not self._initialized:
            super()._initialize_store()
            if not self._dir_path.exists():
                self._dir_path.mkdir(parents=True)
            self._initialized = True

    def exists(self, league: str, season: str, feed: str, data_format: str, params: dict) -> bool:
        self._initialize_store()
        filename: str = self.resolve_filename(league, season, feed, data_format, params)
        file_path: Path = self._dir_path / filename
        return file_path.exists()

    def load(self, league: str, season: str, feed: str, data_format: str, params: dict) -> Any:
        self._initialize_store()
        filename: str = self.resolve_filename(league, season, feed, data_format, params)
        file_path: Path = self._dir_path / filename
        data: Any
        if file_path.exists():
            with file_path.open("r+") as infile:
                data = _load_data(data_format, infile)
        else:
            data = None
        return data

    def store(self, data: Any, league: str, season: str, feed: str, data_format: str, params: dict) -> Path:
        self._initialize_store()
        filename: str = self.resolve_filename(league, season, feed, data_format, params)
        file_path: Path = self._dir_path / filename
        with file_path.open("w+") as file:
            _write_data(data, data_format, file)
        return file_path


class S3Store(DataStore):
    """ AWS S3 Store """

    TYPE_NAME: str = "s3"
    bucket: Any
    prefix: str
    _s3: Any

    def __init__(self, bucket_name: str, prefix: str = None):
        super().__init__()
        self.prefix = prefix
        self._s3 = boto3.resource("s3")
        self.bucket = self._s3.Bucket(bucket_name)

    def _initialize_store(self):
        if not self._initialized:
            super()._initialize_store()
            try:
                self.bucket.load()
            except ClientError as e:
                logging.error("Client error checking for bucket existence")
                logging.error(e)
                raise e
            self._initialized = True

    def exists(self, league: str, season: str, feed: str, data_format: str, params: dict) -> bool:
        self._initialize_store()
        object_key: str = self._get_object_key(league, season, feed, data_format, params)
        try:
            self.bucket.Object(object_key).load()
        except ClientError as e:
            logging.error("Client error")
            logging.error(e)
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                logging.error(f"S3 client error checking for existence of key {object_key}"
                              f" in bucket {self.bucket}")
                return False
        else:
            return True

    def load(self, league: str, season: str, feed: str, data_format: str, params: dict) -> Any:
        self._initialize_store()
        object_key: str = self._get_object_key(league, season, feed, data_format, params)
        s3_object: Any = self.bucket.Object(object_key)

        data: Any
        try:
            s3_response: dict = s3_object.get()
            with s3_response["Body"].read() as input_stream:
                data = _load_data(data_format, input_stream)
        except ClientError as e:
            logging.error(e)
            logging.error(f"S3 client error loading object {object_key} from"
                          f" bucket {self.bucket}: {e}")
            data = None

        return data

    def store(self, data: Any, league: str, season: str, feed: str, data_format: str, params: dict) -> Any:
        self._initialize_store()
        object_key: str = self._get_object_key(league, season, feed, data_format, params)
        s3_object = self.bucket.Object(object_key)

        s3_response: Any
        with _store_temp_file(data, data_format) as temp_file:
            try:
                s3_response = s3_object.upload_fileobj(temp_file)
            except ClientError as e:
                logging.error(f"S3 client error uploading data file {object_key}"
                              f" to bucket {self.bucket}: {e}")
                logging.error(e)
                s3_response = None

        return s3_response

    def _get_object_key(self, league: str, season: str, feed: str, data_format: str, params: dict) -> str:
        filename: str = self.resolve_filename(league, season, feed, data_format, params)
        object_key: str
        if self.prefix is None:
            object_key = filename
        else:
            object_key = f"{self.prefix}/{filename}"
        return object_key


def validate_data_store(data_store: DataStore, store_type: str, store_location: str) -> DataStore:
    """ Provide legacy data storage support for specified parameters. """
    if store_type is not None:
        if store_type == FileStore.TYPE_NAME:
            return FileStore(store_location if not None else DEFAULT_FILE_STORE_DIRECTORY)
        else:
            raise ValueError(f"Unrecognized storage type specified. Supported values are: None, {FileStore.TYPE_NAME}")

    return data_store


def _load_data(data_format: str, input_stream: IO) -> Any:
    """ Loads the data to an input stream. """
    if data_format == "json":
        return json.load(input_stream)
    elif data_format == "xml":
        return input_stream.read()
    elif data_format == "csv":
        reader = csv.reader(input_stream)
        return list(list(rec) for rec in reader)
    else:
        raise AssertionError(f"Invalid data format: {data_format}")


def _write_data(data: Any, data_format: str, output_stream: IO) -> None:
    """ Writes the data to an output stream. """
    if data_format == "json":
        json.dump(data, output_stream)
    elif data_format == "xml":
        output_stream.write(data)
    elif data_format == "csv":
        writer = csv.writer(output_stream)
        for row in data:
            writer.writerow([row])
    else:
        raise AssertionError(f"Invalid data format: {data_format}")

    output_stream.flush()


def _store_temp_file(data: Any, data_format: str) -> NamedTemporaryFile:
    """ Writes the data to a temporary file and returns the file. """
    temp_file: NamedTemporaryFile = NamedTemporaryFile(mode="w+", suffix=f".{data_format}")
    _write_data(data, data_format, temp_file)
    temp_file.seek(0)
    return temp_file
