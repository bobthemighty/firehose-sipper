import io
import json
from json.decoder import JSONDecodeError

import pytest

from .data import schema
from firehose_sipper import object_stream


def make_file(num_objects, trailer=None):
    f = io.StringIO()
    for obj in schema.create(num_objects):
        f.write(json.dumps(obj))
    if trailer:
        f.write(trailer)

    f.seek(0)
    return f


def test_when_sipping_a_single_json_object():

    input = schema.create(1)

    f = io.StringIO(json.dumps(input))
    f.seek(0)

    [output] = list(object_stream(f))

    assert input == output


def test_when_reading_a_large_number_of_objects():

    num = 5000

    f = make_file(num)
    output = list(object_stream(f))
    assert len(output) == num


def test_when_the_file_is_truncated():
    f = make_file(500, "{")

    with pytest.raises(JSONDecodeError):
        list(object_stream(f))


def test_when_the_file_is_malformed():
    f = make_file(500)
    f.seek(1000)
    f.write('"""')
    f.seek(0)

    with pytest.raises(JSONDecodeError):
        list(object_stream(f))
