import s3_handler
from pyspark import SparkContext
import json
from mongo_handler import MongoHandler


sc = SparkContext("local[*]", "Normalizer")

mongo = MongoHandler('localhost', 27017, 'camara')

def match(body, deputados_indexes):
    indexes = []
    for name, index in deputados_indexes.items():
        if name in body:
            indexes.append(index)
    
    return indexes

def get_files():
    deputados_file_list = s3_handler.list_files('00/dadosabertos-camara-deputados')
    news_file_list = s3_handler.list_files('00/camara-news/json')

    rdd_deputados = sc.parallelize(deputados_file_list['Contents'])
    rdd_news = sc.parallelize(news_file_list['Contents'])

    deputados = rdd_deputados.map(lambda file: s3_handler.get_file(file['Key'])).collect()
    news = rdd_news.map(lambda file: s3_handler.get_file(file['Key'])).collect()

    return deputados, news

deputados, news_list = get_files()

deputados_indexes = {}
deputados_new = {}
for i in range(len(deputados)):
    deputado = json.loads(deputados[i])
    deputados_indexes[deputado['ultimoStatus']['nome_normalized']] = i
    deputados_new[deputado['id']] = deputado
    deputados_new[deputado['id']]['news'] = []


for news in news_list:
    news = json.loads(news)
    news['deputados'] = []
    body = news['news']['body_normalized']
    indexes = match(body, deputados_indexes)
    if len(indexes) > 0:
        for i in indexes:
            deputado = json.loads(deputados[i])

        deputados_new[deputado['id']]['news'].append(deputado)

for k, v in deputados_new.items():
    mongo.insert('deputados', v)

