import boto3
import json


## Link de login do console: https://981601120657.signin.aws.amazon.com/console
## Usuario: EngDados
## Senha: engdados@2019

## https://s3.console.aws.amazon.com/s3/buckets/minicurso-engenharia-de-dados/?region=us-east-1&tab=overview

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

    if path.endswith('json'):
        data = json.dumps(data, ensure_ascii=False)
    else:
        data = data.encode('utf8')

    client.put_object(Body=data, Bucket=bucketname, Key=path)

def list_files(folder_path):
    return client.list_objects_v2(Bucket=bucketname, Prefix=folder_path)

def get_file(key):
    obj = s3.Object(bucketname, key)
    return obj.get()['Body'].read().decode('utf-8')
