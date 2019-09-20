import json
import boto3
import s3_handler
from unidecode import unidecode


def normalize(v):
    return unidecode(v.lower())

def extract_values(obj, aux=''):
    normalized_obj = obj.copy()

    for k, v in obj.items():
        if isinstance(v, dict):
            normalized_obj[k] = extract_values(v, k)
        else:
            if isinstance(v, str):
                v = normalize(v)
                normalized_obj[k + '_normalized'] = v

    return normalized_obj

documents = s3_handler.list_files('00/deputados')

for doc in documents['Contents']:
    content = s3_handler.get_file(doc['Key'])
    obj = json.loads(content)
    
    obj = extract_values(obj)
    print(json.dumps(obj, ensure_ascii=False))
    break
