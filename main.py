import boto3
import json
import time
import sys

s3 = boto3.resource("s3")

transfer_config = boto3.s3.transfer.TransferConfig(
    multipart_threshold=5242880,    # 5 MiB
    multipart_chunksize=5242880,    # 5 MiB
    max_concurrency=256             # Number of threads, hits connection limit / throughput wall after 128 or so
)

def callback(byte_count):
    print("copied", byte_count, "bytes")

def s3_copy(parameters):

    print("input", parameters)

    obj = s3.Bucket(parameters["Bucket"]).Object(parameters["Key"])

    start = time.monotonic()

    obj.copy(
        CopySource=parameters["CopySource"],
        ExtraArgs=parameters["ExtraArgs"],
        Callback=callback,
        # SourceClient=None,        # Optional client for source object access (remote account sts assume role creds)
        Config=transfer_config,
    )

    duration = round(time.monotonic() - start, 3)

    print("copied", "/".join([parameters["CopySource"]["Bucket"], parameters["CopySource"]["Key"]]),
        "to", "/".join([parameters["Bucket"], parameters["Key"]]), "in", duration, "s")

if __name__ == "__main__":
    for filename in sys.argv[1:]:
        with open(filename, "rb") as file:
            s3_copy(json.load(file))
