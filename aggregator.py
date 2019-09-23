import s3_handler
from pyspark import SparkContext
import json
from mongo_handler import MongoHandler


sc = SparkContext("local[*]", "Normalizer")
mongo = MongoHandler('localhost', 27017, 'camara')

def get_files():
    deputados_file_list = s3_handler.list_files('00/dadosabertos-camara-deputados')
    news_file_list = s3_handler.list_files('00/camara-news/json')

    rdd_deputados = sc.parallelize(deputados_file_list['Contents'])
    rdd_news = sc.parallelize(news_file_list['Contents'])

    deputados = rdd_deputados.map(lambda file: s3_handler.get_file(file['Key']))\
        .map(lambda x: json.loads(x))\
            .collect()
    news = rdd_news.map(lambda file: s3_handler.get_file(file['Key']))\
        .map(lambda x: json.loads(x))\
            .collect()

    return deputados, news

deputados, news_list = get_files()

for deputado in deputados:
    deputado['news'] = []

for news in news_list:
    body = news['news']['body_normalized']

    for deputado in deputados:
        if deputado['ultimoStatus']['nome_normalized'] in body:
            deputado['news'].append(news)

mongo.insert_many('deputados', deputados)
