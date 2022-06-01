import boto3
import json
import botocore
from boto3.s3.transfer import TransferConfig
import os
import logging
import pandas as pd
from sklearn.model_selection import  GridSearchCV
from sklearn.ensemble import RandomForestClassifier
import pickle

print('Loading function')

s3 = boto3.client('s3')
log = logging.getLogger()

CREDIT_BUCKET = 'bpmn-credit-use-case-us'
INPUT_FOLDER_TEMPLATE = 'data/datasets/splits/train/{}'
OUTPUT_MODEL_FOLDER_TEMPLATE = 'models/tuned/{}'

input_x_train_file = 'X_train.csv.gz'
input_y_train_file = 'y_train.csv.gz'

output_file = 'model.pickle'


def download_data(filename,
                  Bucket = CREDIT_BUCKET, 
                  Input_folder_template = INPUT_FOLDER_TEMPLATE):
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
        s3.download_file(Bucket, Input_folder_template.format(filename), data_file, Config=config)
    except botocore.exceptions.ClientError as e:
        log.error(f'Unable to download data: {filename}')
        log.debug(e)
        raise
    return data_file

def tune_model(X_train, y_train):
    # Fit Random Forest Model with GridSearchCV
    # Data is imbalanced. Not accounted for here, but SMOTE can be used

    rf = RandomForestClassifier()
    parameters = {'n_estimators': [10, 20],#, 200, 500],    
              'max_depth': [5, 10],         
              'random_state': [42]
              }

    # To retain simplicity of the use case, apply GridSearch CV
    # Also possible to define a mapping function that results in a sub-process
    # where each corresponding function receives one set of parameters and training data
    # trains the model, returns results and then the best model is chosen
    # This would speed up computation
    gs = GridSearchCV(rf, parameters, cv=5, scoring='roc_auc', n_jobs=-1, verbose=1)
    
    gs.fit(X_train, y_train)
    
    return(gs.best_estimator_)

def upload_model(results_path, OUTPUT_FOLDER_TEMPLATE, output_file_name):
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
        log.info("Uploaded tmp results to s3://{}/".format(CREDIT_BUCKET) + OUTPUT_FOLDER_TEMPLATE.format(output_file_name))
    except botocore.exceptions.ClientError as e:
        log.error(f'Unable to upload model: {results_path}')
        log.debug(e)
        raise


def lambda_handler(event, context):
    
    # load train data from bucket
    x_train_tmp_path = download_data(input_x_train_file)
    y_train_tmp_path = download_data(input_y_train_file)
    X_train = pd.read_csv(x_train_tmp_path, compression='gzip', 
                    header=0, sep=',', quotechar='"')
    y_train = pd.read_csv(y_train_tmp_path, compression='gzip', 
                    header=0, sep=',', quotechar='"')
    
    rf_model = tune_model(X_train, y_train)
    
    with open('/tmp/model.pickle', 'wb') as handle:
        pickle.dump(rf_model, handle)
    


    upload_model('/tmp/{}'.format(output_file), OUTPUT_MODEL_FOLDER_TEMPLATE, output_file)

    return {
        'statusCode': 200,
        'body': json.dumps('Finished tuning!'),
        'model_path' : OUTPUT_MODEL_FOLDER_TEMPLATE.format(output_file)
    }
