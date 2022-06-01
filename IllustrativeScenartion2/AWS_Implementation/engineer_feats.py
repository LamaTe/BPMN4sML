import json
import urllib.parse
import boto3
from boto3.s3.transfer import TransferConfig
import botocore
import os
import pandas as pd
import logging

print('Loading function')

s3 = boto3.client('s3')
log = logging.getLogger()
CREDIT_BUCKET = 'bpmn-credit-use-case-us'
OUTPUT_FOLDER_TEMPLATE = 'data/datasets/{}'

filename = 'data/featureSets/application_raw.zip'
out_file = 'engineeredDataset'

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
        s3.download_file(Bucket, filename, data_file, Config=config)
    except botocore.exceptions.ClientError as e:
        log.error(f'Unable to download data: {filename}')
        log.debug(e)
        raise
    return data_file
    
def engineerFeats(data):
    pct_null = data.isnull().sum() / len(data)
    missing_features = pct_null[pct_null > 0.50].index
    # remove features with over 50 % of missings
    dat = data.drop(missing_features, axis=1)

    # further remove noisy features
    dat = data.drop(['SK_ID_CURR', 'NAME_CONTRACT_TYPE', 'EXT_SOURCE_2', 'EXT_SOURCE_3', 
                    'AMT_REQ_CREDIT_BUREAU_HOUR', 'AMT_REQ_CREDIT_BUREAU_DAY', 
                    'AMT_REQ_CREDIT_BUREAU_WEEK', 'AMT_REQ_CREDIT_BUREAU_MON', 'AMT_REQ_CREDIT_BUREAU_QRT',
                    'AMT_REQ_CREDIT_BUREAU_YEAR', 'EMERGENCYSTATE_MODE', 'YEARS_BEGINEXPLUATATION_AVG', 
                    'FLOORSMAX_AVG', 'YEARS_BEGINEXPLUATATION_MODE', 'FLOORSMAX_MODE',
                    'YEARS_BEGINEXPLUATATION_MEDI', 'FLOORSMAX_MEDI', 'TOTALAREA_MODE',
                    'OCCUPATION_TYPE', 'DAYS_EMPLOYED'], axis = 1)
    
    # encode categorical missings as unknown
    dat = dat.apply(lambda x: x.fillna('Unknown') if x.dtype.kind in 'O' else x)
    
    # drop features with too many categories for this example (otherwise one hot encoding creates extreme sparsity)
    cat_cols = list(dat.select_dtypes('object').apply(pd.Series.nunique, axis = 0).nlargest(2).index)
    dat.drop(cat_cols, axis=1, inplace = True)
    # one hot encode the rest
    dat = pd.get_dummies(dat)
    return(dat)

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
            CREDIT_BUCKET,
            OUTPUT_FOLDER_TEMPLATE.format(output_file_name))
        log.info("Uploaded temp results to s3://{}/".format(CREDIT_BUCKET) + OUTPUT_FOLDER_TEMPLATE.format(output_file_name))
    except botocore.exceptions.ClientError as e:
        log.error(f'Unable to upload intermediate results: {results_path}')
        log.debug(e)
        raise

    
def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    #bucket = event['Records'][0]['s3']['bucket']['name']
    #key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        #response = s3.get_object(Bucket=bucket, Key=key)
        data = download_data(filename)
        df_data = pd.read_csv(data, compression='zip', 
                    header=0, sep=',', quotechar='"')
                    
        engineeredData = engineerFeats(df_data)
        
        # write to file
        output_file_name = "{}.csv.gz".format(out_file)
        output_file = '/tmp/{}'.format(output_file_name)
        engineeredData.to_csv(output_file, compression='gzip', 
                        index= False, header = True)
        
        # upload to target S3 bucket
        upload_dataset(output_file, output_file_name)
        storage_path = CREDIT_BUCKET + '/'+ OUTPUT_FOLDER_TEMPLATE.format(output_file_name)
        #print("CONTENT TYPE: " + response['ContentType'] + " " + output_file_name)
        print(storage_path)
        return{'statusCode': 200,
                'storage_path' : storage_path}
    except Exception as e:
        print(e)
        #print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        print('Error downloading object {} from bucket {}.'.format(filename, CREDIT_BUCKET))
        raise e
