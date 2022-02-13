import io
import json
import os
from uuid import uuid4

import boto3
import pytest

from .data import schema

from firehose_sipper import sip

S3 = boto3.client("s3", endpoint_url=os.environ.get("SIPPER_S3_ENDPOINT"))
BUCKET = os.environ.get("SIPPER_BUCKET_NAME")
PREFIX = str(uuid4())


def test_when_the_object_exists():
    num = 5000

    key = f"{PREFIX}/when_the_object_exists"
    data = io.BytesIO()

    for entry in schema.create(num):
        data.write(json.dumps(entry).encode("utf8"))
    data.seek(0)

    S3.put_object(Bucket=BUCKET, Key=key, Body=data)

    result = list(sip(bucket=BUCKET, key=key, s3=S3))

    assert len(result) == num


def test_when_requesting_both_a_key_and_prefix():
    key = f"{PREFIX}/when_the_object_exists"
    msg = "You must provide either a key or prefix"

    with pytest.raises(ValueError, match=msg) as e:
        list(sip(bucket=BUCKET, key=key, prefix=PREFIX))


def test_when_requesting_neither_a_key_nor_prefix():
    key = f"{PREFIX}/when_the_object_exists"
    msg = "You must provide either a key or prefix"

    with pytest.raises(ValueError, match=msg) as e:
        list(sip(bucket=BUCKET))


def test_when_reading_a_prefix():
    PREFIX = str(uuid4())
    for entry in schema.create(200):
        data = json.dumps(entry).encode("utf8")
        S3.put_object(Bucket=BUCKET, Key=f"{PREFIX}/{uuid4()}", Body=data)

    result = list(sip(bucket=BUCKET, prefix=PREFIX, s3=S3))

    assert len(result) == 200
