import boto3

def remote_client(sts_client, role_arn, service_name):

    creds = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=service_name + "-access"
    )["Credentials"]

    return boto3.client(
        service_name=service_name,
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"]
    )
