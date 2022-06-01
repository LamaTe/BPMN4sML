import json
import urllib.parse
import boto3
from boto3.s3.transfer import TransferConfig
import botocore
import os
import logging

print('Loading function')

s3 = boto3.client('s3')
log = logging.getLogger()
PUBLIC_BUCKET = 'home-credit-default-prediction-data'
OUTPUT_BUCKET = os.environ['CREDIT_BUCKET']
OUTPUT_FOLDER_TEMPLATE = 'data/featureSets/{}'

filename = 'application_raw.zip'

def download_data(filename, Bucket = PUBLIC_BUCKET):
    """Download a file from S3
    Parameters
    ----------
    filename: string, required
        Name of the file in S3 source bucket (OpenAQ)
    Returns
    -------
    data_file: string
        Local path to downloaded file
    """

    try:
        config = TransferConfig(max_concurrency=2)
        data_file = os.path.join('/tmp', os.path.basename(filename))
        s3.download_file(Bucket, filename, data_file, Config=config)
    except botocore.exceptions.ClientError as e:
        log.error(f'Unable to download data: {filename}')
        log.debug(e)
        raise
    return data_file
    


def upload_dataset(results_path, output_file_name):
    """Upload a file to S3
    Parameters
    ----------
    results_path: string, required
        Name of the local file path with the training dataset
    """

    # upload to target S3 bucket
    try:
        response = s3.upload_file(
            results_path,
            OUTPUT_BUCKET,
            OUTPUT_FOLDER_TEMPLATE.format(output_file_name))
        log.info("Uploaded temp results to s3://{}/".format(OUTPUT_BUCKET) + OUTPUT_FOLDER_TEMPLATE.format(output_file_name))
    except botocore.exceptions.ClientError as e:
        log.error(f'Unable to upload intermediate results: {results_path}')
        log.debug(e)
        raise

    
def main(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    #bucket = event['Records'][0]['s3']['bucket']['name']
    #key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        #response = s3.get_object(Bucket=bucket, Key=key)
        data_path = download_data(filename)
                    
        # upload to target S3 bucket
        upload_dataset(data_path, filename)
        storage_path = OUTPUT_BUCKET + '/'+ OUTPUT_FOLDER_TEMPLATE.format(filename)
        #print("CONTENT TYPE: " + response['ContentType'] + " " + output_file_name)
        print(storage_path)
        return{'statusCode': 200,
                'storage_path' : storage_path}
    except Exception as e:
        print(e)
        #print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        print('Error downloading object {} from bucket {}.'.format(filename, OUTPUT_BUCKET))
        raise e
