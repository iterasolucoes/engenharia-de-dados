import s3_handler
from bs4 import BeautifulSoup
import json


def get_S3_file(filename: str):
    data_file = s3_handler.get_file(filename)
    extract_info(data_file)


def load_files():
    files = s3_handler.list_files('teste')
    [get_S3_file(file['Key']) for file in files['Contents']]


def extract_info(content: str):
    soup = BeautifulSoup(content, 'html.parser')
    title: str = soup.title.text.strip()
    date = soup.find('p', attrs={'class': 'g-artigo__data-hora'}).text.strip()
    article = soup.find('div', attrs={'class': 'js-article-read-more'})

    references = [{'link': reference['href'], 'label':reference.text}
                  for reference in article.find_all('a')]

    news_link = soup.find('meta', attrs={'property': 'og:url'})['content']

    news = {'body': article.text, 'link': news_link}

    category = soup.find('span', {'class': 'g-artigo__categoria'}).text
    proposals = [{'text': li.a.text, 'link': li.a['href']}
                 for li in soup.find_all('li', {'class': 'integra-lista__item'})]

    data = {
        'title': title,
        'date': date,
        'references': references,
        'news': news,
        'category': category,
        'proposals': proposals
    }

    filename = news_link.split(
        '/')[-2] if news_link.endswith('/') else news_link.split('/')[-1]

   s3_handler.upload_file(data, '00/camara-news/json', filename+'.json')


load_files()
