from typing import List, Dict
import requests
from bs4 import BeautifulSoup
import os
import s3_handler


def get_news_link(page_num: int) -> List[str]:

    content = requests.get(
        f'https://www.camara.leg.br/noticias/ultimas?pagina={page_num}')

    soup = BeautifulSoup(content.text, 'html.parser')
    h3_elements = soup.find_all(
        'h3', {'class': 'g-chamada__titulo'})

    links = [element.a.get('href') for element in h3_elements]
    return links


def get_news(link: str) -> None:
    print(link)
    content = requests.get(link)

    filename = link.split(
        '/')[-2] if link.endswith('/') else link.split('/')[-1]

    s3_handler.upload_file(content.text, '00/camara-news/html', filename+'.html')


def recursively(page_num):
    links = get_news_link(page_num)
    print(page_num)
    if len(links) > 0 or page_num < 101:
        [get_news(link) for link in links]
        recursively(page_num+1)


recursively(1)
