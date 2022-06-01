import boto3
import json
import botocore
from boto3.s3.transfer import TransferConfig
import os
import logging
import pandas as pd
from sklearn.metrics import roc_auc_score
from sklearn.ensemble import RandomForestClassifier
import pickle

print('Loading function')

s3 = boto3.client('s3')
log = logging.getLogger()

CREDIT_BUCKET = 'bpmn-credit-use-case-us'
INPUT_DATA_FOLDER_TEMPLATE = 'data/datasets/splits/test/{}'
INPUT_MODEL_FOLDER_TEMPLATE = 'models/tuned/{}'

#OUTPUT_MODEL_FOLDER_TEMPLATE = 'models/final/{}'

input_x_test_file = 'X_test.csv.gz'
input_y_test_file = 'y_test.csv.gz'
model_file = 'model.pickle'
#final_model = 'final_model'

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

def verify_model(X_test, y_test, model):
    # score best performing model on test data
    # alternatively tuned model could also be retrained on entire dataset beforehand
    y_test_proba = model.predict_proba(X_test)
    score = roc_auc_score(y_test, y_test_proba[:, 1])
    return(score)



def main(event, context):
    
    # load train data from bucket
    X_test_tmp_path = download_objects(input_x_test_file, INPUT_DATA_FOLDER_TEMPLATE)
    y_test_tmp_path = download_objects(input_y_test_file, INPUT_DATA_FOLDER_TEMPLATE)
    model_tmp_path = download_objects(model_file, INPUT_MODEL_FOLDER_TEMPLATE)
    X_test = pd.read_csv(X_test_tmp_path, compression='gzip', 
                    header=0, sep=',', quotechar='"')
    y_test = pd.read_csv(y_test_tmp_path, compression='gzip', 
                    header=0, sep=',', quotechar='"')
    # load tuned model to train a final time on entire dataset
    with open(model_tmp_path, 'rb') as handle:
        model = pickle.load(handle)
    
    score = verify_model(X_test, y_test, model)

    # store and upload
    #with open('/tmp/{}'.format(final_model), 'wb') as handle:
    #    pickle.dump(model, handle)

    #upload_model('/tmp/{}'.format(final_model), OUTPUT_MODEL_FOLDER_TEMPLATE, 
    #                final_model)

    return {
        'statusCode': 200,
        'body': json.dumps('Finished scoring tuned model'),
        'roc_auc_score' : score, 
        'model_path' : INPUT_MODEL_FOLDER_TEMPLATE.format(model_file)
    }
