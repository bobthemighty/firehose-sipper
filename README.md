# What's this?

A tiny library for reading concatenated json blobs out of S3.

## Why on earth would I want that?

Kinesis firehose, by default, writes json to files in S3 with no delimiter. Eg. if you have json blobs of the form `{key: value}`and firehose writes three of them to the same file, you'll end up with the string `{key: value}{key:value}{key:value}`.

The default `json.load()` function in Python treats such files as invalid JSON, not without justification.

## How do I use it?

```python
import firehose_sipper

# Read a single file out of S3

for entry in firehose_sipper.sip(bucket=some_bucket, key=some_key):
    # Each entry is a dict, parsed from a json object
    print(entry)
    
# or go nuts and read all objects under a prefix
for entry in firehose_sipper.sip(bucket=some_bucket, prefix=some_prefix):
    print(entry)
```

The library respects `gzip` encoding automatically, so you can point it at a bucket and start processing.
