import chardet
import csv
from contextlib import contextmanager
import io
import s3fs
from botocore.exceptions import ClientError, NoCredentialsError

def open_csv_utf8_in_memory(filepath):
    """
    Reads a CSV file, auto-detects encoding, decodes to UTF-8, and returns a StringIO object.
    All downstream code can use this as a file-like object.

    Doctest:
    >>> import tempfile
    >>> import os
    >>> import pandas as pd
    >>> # UTF-8
    >>> with tempfile.NamedTemporaryFile('w', delete=False, encoding='utf-8', suffix='.csv') as f:
    ...     _ = f.write('a,b\\n1,2\\n')
    ...     fname = f.name
    >>> sio = open_csv_utf8_in_memory(fname)
    >>> df = pd.read_csv(sio)
    >>> list(df.columns)
    ['a', 'b']
    >>> os.remove(fname)

    >>> with tempfile.NamedTemporaryFile('w', delete=False, encoding='latin1', suffix='.csv') as f:
    ...     _ = f.write('c,d\\n3,4\\n')
    ...     fname = f.name
    >>> sio = open_csv_utf8_in_memory(fname)
    >>> df = pd.read_csv(sio)
    >>> list(df.columns)
    ['c', 'd']
    >>> os.remove(fname)

    Reads a CSV file, auto-detects encoding, decodes to UTF-8, and returns a StringIO object.
    All downstream code can use this as a file-like object.
    """

    with open(filepath, "rb") as f:
        raw = f.read()
    enc = chardet.detect(raw)["encoding"] or "utf-8"
    text = raw.decode(enc, errors="replace")
    return io.StringIO(text)


@contextmanager
def open_file(
    file_path,
    mode='r',
    encoding='utf-8',
    aws_access_key_id=None,
    aws_secret_access_key=None,
    aws_session_token=None,
):
    """
    A context manager to open local or S3 files.
    
    Streams S3 files for memory efficiency and handles AWS authentication.
    """
    if file_path.startswith('s3://'):
        try:
            s3 = s3fs.S3FileSystem(
                key=aws_access_key_id,
                secret=aws_secret_access_key,
                token=aws_session_token,
            )
            s3_file = s3.open(file_path, "rb")
            f = TextIOWrapper(s3_file, encoding=encoding)
        except (ClientError, NoCredentialsError) as e:
            raise ValueError(f"S3 authentication failed: {e}")
        except FileNotFoundError:
            raise ValueError(f"S3 file not found at path: {file_path}")
    else:
        try:
            f = open_csv_utf8_in_memory(file_path)
        except FileNotFoundError:
            raise ValueError(f"Local file not found at path: {file_path}")

    try:
        yield f
    finally:
        if f:
            f.close()


def get_csv_header(file_path, **kwargs):
    """
    Reads the header row from a local or S3 CSV file.
    **kwargs are passed to open_file for S3 authentication.
    """
    with open_file(file_path, **kwargs) as f:
        reader = csv.reader(f)
        try:
            return next(reader)
        except StopIteration:
            return []


def get_csv_row_count(file_path, **kwargs):
    """
    Counts data rows in a local or S3 CSV file (excluding header).
    **kwargs are passed to open_file for S3 authentication.
    """
    with open_file(file_path, **kwargs) as f:
        next(f, None)
        return sum(1 for _ in f)


def csv_to_dicts_chunked(
    filepath,
    chunksize=1000,
    aws_access_key_id=None,
    aws_secret_access_key=None,
    aws_session_token=None,
):
    """
    Reads a CSV file in chunks and returns lists of row dicts.
    Empty strings are normalized to None.

    Used for reading data dictionaries in chunks to avoid memory issues with large files.
    If reading the entire file into memory is not necessary(most cases), use open_file to
    stream the file directly.
    """
    auth_keys = {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "aws_session_token": aws_session_token,
    }
    try:
        with open_file(filepath, **auth_keys) as f:
            reader = csv.DictReader(f)
            chunk = []
            for row in reader:
                chunk.append({k: (v if v != "" else None) for k, v in row.items()})
                if len(chunk) == chunksize:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk
    except csv.Error as e:
        raise ValueError(f"CSV parsing error on line {reader.line_num}: {e}") from e
