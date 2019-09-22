import requests
import json
import s3_handler
import random


url_deputados = 'https://dadosabertos.camara.leg.br/api/v2/deputados'

def get_deputados():
    response = requests.get(url_deputados)
    
    if response.status_code == 200:
        obj = json.loads(response.text)
        random.shuffle(obj['dados'])
        [get_detalhe_deputado(deputado['id']) for deputado in obj['dados'][:100]]

def get_detalhe_deputado(deputado_id):
    url = f'{url_deputados}/{deputado_id}'
    response = requests.get(url)

    if response.status_code == 200:
        deputado = json.loads(response.text)

        s3_handler.upload_file(deputado['dados'], '00/dadosabertos-camara-deputados',
                               str(deputado['dados']['id']) + '.json')

get_deputados()
