import boto3
#from botocore import UNSIGNED
#from botocore.client import Config
import botocore
from boto3.s3.transfer import TransferConfig
import os
import logging
import pandas as pd
import json
from sklearn.model_selection import train_test_split

print('Loading function')
s3 = boto3.client('s3')
log = logging.getLogger()

CREDIT_BUCKET = 'bpmn-credit-use-case-us'
INPUT_FOLDER_TEMPLATE = 'data/datasets/{}'
OUTPUT_TRAIN_FOLDER_TEMPLATE = 'data/datasets/splits/train/{}'
OUTPUT_TEST_FOLDER_TEMPLATE = 'data/datasets/splits/test/{}'

input_filename = 'engineeredDataset.csv.gz'

tmp_path = '/tmp/{}.csv.gz'
generic_upload_name = '{}.csv.gz'

def download_data(filename, Bucket = CREDIT_BUCKET):
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
        s3.download_file(Bucket, INPUT_FOLDER_TEMPLATE.format(filename), data_file, Config=config)
    except botocore.exceptions.ClientError as e:
        log.error(f'Unable to download data: {filename}')
        log.debug(e)
        raise
    return data_file
    
def split_data(data):
    # Set feature matrix X and Target vector y
    y_data = data['TARGET']
    X_data = data.drop(columns = ['TARGET'])


    # Do train_test_split() with stratify parameter because data is imbalanced
    X_train, X_test, y_train, y_test = train_test_split(X_data, y_data, stratify=y_data, test_size=0.20, random_state=42)
    
    # For Training data - Fill missing values for all float and int columns with respective column median  
    X_train = X_train.apply(lambda x: x.fillna(x.median()) if x.dtype.kind in 'fi' else x)

    # Repeat for Test data   
    X_test = X_test.apply(lambda x: x.fillna(x.median()) if x.dtype.kind in 'fi' else x)
    
    return (X_train, X_test, y_train, y_test)

def upload_dataset(results_path, OUTPUT_FOLDER_TEMPLATE, output_file_name):
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
            OUTPUT_FOLDER_TEMPLATE.format(output_file_name))
        log.info("Uploaded temp results to s3://{}/".format(CREDIT_BUCKET) + OUTPUT_FOLDER_TEMPLATE.format(output_file_name))
    except botocore.exceptions.ClientError as e:
        log.error(f'Unable to upload intermediate results: {results_path}')
        log.debug(e)
        raise

    
def lambda_handler(event, context):
    # download file locally, unzip and load into function
    tmp_file_path = download_data(input_filename, Bucket = CREDIT_BUCKET)
    df_data = pd.read_csv(tmp_file_path, compression='gzip', 
                    header=0, sep=',', quotechar='"')
    X_train, X_test, y_train, y_test = split_data(df_data)
    
    X_train.to_csv('/tmp/X_train.csv.gz', compression='gzip', index= False, header = True)
    X_test.to_csv('/tmp/X_test.csv.gz', compression='gzip', index= False, header = True)
    y_train.to_csv('/tmp/y_train.csv.gz', compression='gzip', index= False, header = True)
    y_test.to_csv('/tmp/y_test.csv.gz', compression='gzip', index= False, header = True)

    upload_dataset(tmp_path.format('X_train'), OUTPUT_TRAIN_FOLDER_TEMPLATE,
                    generic_upload_name.format('X_train')) 
    upload_dataset(tmp_path.format('y_train'), OUTPUT_TRAIN_FOLDER_TEMPLATE,
                    generic_upload_name.format('y_train')) 
    upload_dataset(tmp_path.format('X_test'), OUTPUT_TEST_FOLDER_TEMPLATE,
                    generic_upload_name.format('X_test')) 
    upload_dataset(tmp_path.format('y_test'), OUTPUT_TEST_FOLDER_TEMPLATE,
                    generic_upload_name.format('y_test')) 

    return {
        'statusCode': 200,
        'body': json.dumps('Data splits have been uploaded'),
        'X_train' : OUTPUT_TRAIN_FOLDER_TEMPLATE.format(generic_upload_name.format('X_train')),
        'y_train' : OUTPUT_TRAIN_FOLDER_TEMPLATE.format(generic_upload_name.format('y_train')),
        'X_test' : OUTPUT_TEST_FOLDER_TEMPLATE.format(generic_upload_name.format('X_test')),
        'y_test' : OUTPUT_TEST_FOLDER_TEMPLATE.format(generic_upload_name.format('y_test'))
    }
