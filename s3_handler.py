import boto3
import json


client = boto3.client(
    's3',
    aws_access_key_id='AKIA6JC74OGIXONPB5UP',
    aws_secret_access_key='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'
)

s3 = boto3.resource(
    's3',
    aws_access_key_id='AKIA6JC74OGIXONPB5UP',
    aws_secret_access_key='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'
)

bucketname = 'minicurso-engenharia-de-dados'

def upload_file(data, folder_path, filename):
    path = f'{folder_path}/{filename}'

    if filename.endswith('json'):
        data = json.dumps(data, ensure_ascii=False)
    else:
        data = data.encode('utf8')

    client.put_object(Body=data, Bucket=bucketname, Key=path)

def list_files(folder_path):
    return client.list_objects_v2(Bucket=bucketname, Prefix=folder_path)

def get_file(key):
    obj = s3.Object(bucketname, key)
    return obj.get()['Body'].read().decode('utf-8')