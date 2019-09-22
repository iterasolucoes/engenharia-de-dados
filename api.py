from mongo_handler import MongoHandler
from flask import Flask
from flask import jsonify


mongo = MongoHandler('localhost', 27017, 'camara')
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False


@app.route('/news/ranking', methods = ['GET'])
def get_deputados_news_ranking():
    deputados = mongo.find_all('deputados')

    data = []
    for deputado in deputados:
        data.append({ 
            'id': deputado['id'],
            'name': deputado['nomeCivil'],
            'partido': deputado['ultimoStatus']['siglaPartido'],
            'news_count': len(deputado['news'])
        })
    return jsonify(sorted(data, key=lambda x: x['news_count'], reverse=True))

if __name__ == '__main__':
    app.run()