import boto3
import logging
import json
import time
import sys

from utils import remote_client

logging.basicConfig(level=logging.INFO)

s3 = boto3.resource("s3")
sts = boto3.client("sts")

transfer_config = boto3.s3.transfer.TransferConfig(
    multipart_threshold=5242880,    # 5 MiB
    multipart_chunksize=5242880,    # 5 MiB
    max_concurrency=256             # Number of threads, hits connection limit / throughput wall after 128 or so
)

def callback(byte_count):
    print("copied", byte_count, "bytes")

def s3_copy(parameters):

    print("input", parameters)

    if "SourceRoleArn" in parameters:
        source_client = remote_client(sts, parameters["SourceRoleArn"], "s3")
    else:
        source_client = None

    obj = s3.Bucket(parameters["Bucket"]).Object(parameters["Key"])

    start = time.monotonic()

    obj.copy(
        CopySource=parameters["CopySource"],
        ExtraArgs=parameters["ExtraArgs"],
        Callback=callback,
        SourceClient=source_client, # The client to be used for operation that may happen at the source object
        Config=transfer_config,
    )

    duration = round(time.monotonic() - start, 3)

    print("copied", "/".join([parameters["CopySource"]["Bucket"], parameters["CopySource"]["Key"]]),
        "to", "/".join([parameters["Bucket"], parameters["Key"]]), "in", duration, "s")

if __name__ == "__main__":
    for filename in sys.argv[1:]:
        with open(filename, "rb") as file:
            s3_copy(json.load(file))
