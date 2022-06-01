import boto3
import json
import botocore
from boto3.s3.transfer import TransferConfig
import os
import logging

print('Loading function')

s3 = boto3.client('s3')
log = logging.getLogger()

CREDIT_BUCKET = os.environ['CREDIT_BUCKET']
INPUT_MODEL_FOLDER_TEMPLATE = 'models/tuned/{}'
OUTPUT_MODEL_FOLDER_TEMPLATE = 'models/final/{}'

model_file = 'model.pickle'

def download_objects(filename,
                  Input_folder_template,
                  Bucket = CREDIT_BUCKET):
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
        object_file = os.path.join('/tmp', os.path.basename(filename))
        s3.download_file(Bucket, Input_folder_template.format(filename), object_file, Config=config)
    except botocore.exceptions.ClientError as e:
        log.error(f'Unable to download data: {filename}')
        log.debug(e)
        raise
    return object_file

def deploy_model(results_path, output_file_name):
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
            CREDIT_BUCKET,
            OUTPUT_MODEL_FOLDER_TEMPLATE.format(output_file_name))
        log.info("Uploaded model to s3://{}/".format(CREDIT_BUCKET) + OUTPUT_MODEL_FOLDER_TEMPLATE.format(output_file_name))
    except botocore.exceptions.ClientError as e:
        log.error(f'Unable to make model available on bucket: {results_path}')
        log.debug(e)
        raise
    

def main(event, context):
    
    # downloads model and deploys it by writing it to the correct bucket / location
    # this can alternatively also be handled by a specialized task within aws StepFunctions, however 
    # having a serverless function is more universal / illustrative from BPMN point of view and can be supported by TOSCA
    model_tmp_path = download_objects(model_file, INPUT_MODEL_FOLDER_TEMPLATE)
    deploy_model(model_tmp_path, model_file)
    
    return {
        'statusCode': 200,
        'body': json.dumps('The final model was successfully deployed!'),
        'deployment location' : "s3://{}/".format(CREDIT_BUCKET) + OUTPUT_MODEL_FOLDER_TEMPLATE.format(model_file) 
    }
