import gzip
import io
from json import scanner
from json.decoder import JSONDecoder, JSONDecodeError

import boto3

GZIP_AUTO = object()


def object_stream(f, decoder=None, bufsize=64 * 1024):
    idx = 0
    data = ""
    decoder = decoder or JSONDecoder()

    scan = scanner.make_scanner(decoder)

    while True:
        read = f.read(bufsize)
        data = data[idx:]

        if not data and not read:
            return
        if data and not read:
            raise JSONDecodeError("Truncated JSON", data, len(data))

        data += read
        idx = 0

        while True:
            try:
                obj, idx = scan(data, idx)
                yield obj
            except StopIteration:
                break
            except JSONDecodeError:
                if idx == 0:
                    raise
                break


def list_files(s3, bucket, prefix):
    files = {
        "IsTruncated": True,
    }

    while files["IsTruncated"]:
        if "NextContinuationToken" in files:
            files = s3.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                ContinuationToken=files["NextContinuationToken"],
            )
        else:
            files = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" in files:
            yield from [f["Key"] for f in files["Contents"]]
        else:
            return


def stream(bucket, key, s3, use_gzip):
    use_gzip = key.endswith(".gz") if use_gzip == GZIP_AUTO else use_gzip
    print(use_gzip)
    data = s3.get_object(Key=key, Bucket=bucket)
    if use_gzip:
        return gzip.open(data["Body"], mode="rt")
    return io.TextIOWrapper(data["Body"])


def sip(*, bucket=None, prefix=None, key=None, s3=None, gzip=GZIP_AUTO):

    if (key and prefix) or not (key or prefix):
        raise ValueError("You must provide either a key or prefix")

    s3 = s3 or boto3.client("s3")

    if key:
        yield from object_stream(stream(bucket, key, s3, gzip))

    else:
        for key in list_files(s3, bucket, prefix):
            yield from object_stream(stream(bucket, key, s3, gzip))
