import logging
import boto3
from botocore.exceptions import ClientError
import json

def upload_file(data, file_name, bucket, object_name=None, ExtraArgs={'ContentType': 'application/json'}):
    """Upload a file to an S3 bucket
    :param data: Data to upload
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    with open(file_name, 'w') as f:
        f.write(json.dumps(data, indent=4))

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name, ExtraArgs=ExtraArgs)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def download_file(file_name, bucket, object_name=None):
    """Download a file from an S3 bucket

    :param file_name: File name to save
    :param bucket: Bucket to download
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was downloaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name
    s3_client = boto3.client('s3')
    try:
        response = s3_client.download_file(bucket, object_name, file_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True