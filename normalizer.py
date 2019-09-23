import json
import s3_handler
from unidecode import unidecode
from pyspark import SparkContext

sc = SparkContext("local[*]", "Normalizer")

def get_filename(key):
    splited = key.split('/')
    return splited[len(splited) - 1]

def normalize(v):
    return unidecode(v.lower())

def normalize_deputados(doc):
    print(doc['Key'])
    content = s3_handler.get_file(doc['Key'])
    obj = json.loads(content)

    obj['nomeCivil_normalized'] = normalize(obj['nomeCivil'])
    obj['ultimoStatus']['nome_normalized'] = normalize(obj['ultimoStatus']['nome'])
    obj['ultimoStatus']['siglaPartido_normalized'] = normalize(obj['ultimoStatus']['siglaPartido'])

    filename = get_filename(doc['Key'])
    s3_handler.upload_file(obj, '00/dadosabertos-camara-deputados', filename)

def normalize_news(doc):
    print(doc['Key'])
    content = s3_handler.get_file(doc['Key'])
    obj = json.loads(content)
    obj['news']['body_normalized'] = normalize(obj['news']['body'])

    filename = get_filename(doc['Key'])
    s3_handler.upload_file(obj, '00/camara-news/json', filename)

deputados_file_list = s3_handler.list_files('00/dadosabertos-camara-deputados')
news_file_list = s3_handler.list_files('00/camara-news/json')

rdd_deputados = sc.parallelize(deputados_file_list['Contents'])
rdd_news = sc.parallelize(news_file_list['Contents'])


rdd_deputados.map(lambda doc: normalize_deputados(doc)).collect()
rdd_news.map(lambda doc: normalize_news(doc)).collect()
