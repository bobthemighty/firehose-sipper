import gzip
import io
from json import scanner
from json.decoder import JSONDecoder, JSONDecodeError

import boto3

GZIP_AUTO = object()


def object_stream(f, decoder=None, bufsize=64 * 1024):
    """Reads a file-like object and returns a generator of decoded json objects

    This function takes a file-like object in text-mode and parses json objects
    one at a time, yielding them in a generator. If the file is truncated, or
    otherwise malformed, we raise a JSONDecodeError.

    Args:
        f: File-like object containing json objects
        decoder: Optional JSONDecoder to customise json handling
        bufsize: Number of bytes to read at once from the file

    Example:
        >>> for object in object_stream(f):
        >>>     print(object)

    """
    idx = 0
    data = ""
    decoder = decoder or JSONDecoder()

    scan = scanner.make_scanner(decoder)

    while True:
        read = f.read(bufsize)
        data = data[idx:]

        if not data and not read:
            # No data and nothing new from the stream? End of file
            return
        if data and not read:
            # unparsed data, but nothing new on the stream?
            # The file is truncated
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
                # If there's an error at index 0, then we don't have
                # valid JSON, otherwise, this is probably half an object
                # and the rest of the data will come in the next chunk.
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
    data = s3.get_object(Key=key, Bucket=bucket)
    if use_gzip == GZIP_AUTO:
        use_gzip = data.get("ContentEncoding") == "gzip"

    if use_gzip:
        return gzip.open(data["Body"], mode="rt")

    return io.TextIOWrapper(data["Body"])


def sip(*, bucket=None, prefix=None, key=None, s3=None, gzip=GZIP_AUTO, decoder=None):
    """Reads a file, or set of files, from S3 and yields decoded JSON objects

    Args:
        bucket: The name of the S3 bucket to read from
        prefix: Optional prefix for listing files
        key: The key of a single file to parse

        s3: An optional instance of boto3.client('s3') for querying data
        gzip: Optional boolean to force reading files as gzip-encoded
        decoder: Optional JSONDecoder for customising JSON processing

    Examples:

        Read a single S3 object

        >>> for object in sip(bucket='my-bucket', key='some-object'):
        >>>     print(object)


        Read all objects under a prefix

        >>> for object in sip(bucket='my-bucket', prefix='some-prefix/'):
        >>>     print(object)
    """

    if (key and prefix) or not (key or prefix):
        raise ValueError("You must provide either a key or prefix")

    s3 = s3 or boto3.client("s3")

    if key:
        yield from object_stream(stream(bucket, key, s3, gzip), decoder=decoder)

    else:
        for key in list_files(s3, bucket, prefix):
            yield from object_stream(stream(bucket, key, s3, gzip), decoder=decoder)
