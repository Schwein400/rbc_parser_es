import requests
import json
from bs4 import BeautifulSoup as BS
from elasticsearch import Elasticsearch
import base64
from datetime import datetime
from deepdiff import DeepDiff
import time
import os
from dotenv import load_dotenv

def new_record(url, modif_date, headers) -> dict:
    '''
        Function to create a new record
    '''
    html_news = requests.get(url, headers)
    soup_news = BS(html_news.text, features='html.parser')

    publication_date = soup_news.find(class_="article__header__date")['datetime']
    
    header = soup_news.find(class_="article__header__title-in").text.strip()
    header = header.replace(u'\xa0', u' ')
    print(f"recording '{header}' ...")
    
    # article may not have a subheader
    try:
        subheader = soup_news.find(class_="article__header__yandex").text
    except:
        subheader = None
    
    # finding main text and pictures location in the document
    article_data = soup_news.find(class_="article__text article__text_free")
    
    # article may not have an overview
    try:
        overview = article_data.find(class_="article__text__overview").text
        overview = overview[1:-1]
        overview = overview.replace(u'\xa0', u' ')
    except:
        overview = None

    # article may not have a picture
    try:
        pic = article_data.find(class_="article__main-image__wrap").picture.img['src'] # url of the picture
        
        #pic_response = requests.get(pic)
        #pic = base64.b64encode(pic_response.content)    # could save pictures
        #pic = pic.decode()                              # as base64 encoded strings
    
    except:
        pic = None
        
    # deleting a trash for simplicity
    for tag in article_data.select('p > div[class~=article__clear]'):
        if tag.parent != None:
            tag.parent.decompose()
    
    # creation of nice text representation
    text = ''
    for tag in article_data.find_all(['p', 'h2', 'li']):
        text += tag.text + '\n\n'
    text = text[:-2]
    text = text.replace(u'\xa0', u' ')
    
    # result JSON document containing null values (if exsist)
    article_dict = {
        'header': header,
        'subheader': subheader,
        'pub_date': publication_date,
        'overview': overview,
        'picture': pic,
        'article_text': text,
        'modif_date': modif_date
    }
    
    # result JSON documetn without null values
    article_dict_new = {k: v for k, v in article_dict.items() if v is not None}
    
    return article_dict_new

while True:
    # connecting to ElasticSearch database
    load_dotenv()
    
    client = Elasticsearch(
        "https://localhost:9200",
        ca_certs = os.getenv('PATH_TO_LOCAL_CERTS'),
        api_key = os.getenv('LOCAL_DB_API_KEY'),
    ) # for local DB
    
    #client = Elasticsearch(
    #    "https://192.168.1.11:9200",
    #    ca_certs = os.getenv('PATH_TO_REMOTE_CERTS'),
    #    api_key = os.getenv('REMOTE_DB_API_KEY'),
    #) # for remote DB

    index = "rbc_top_news_index"
    history_index = "rbc_top_news_history_index"

    st_accept = "text/html"
    st_useragent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    headers = {
       "Accept": st_accept,
       "User-Agent": st_useragent
    }

    print('Checking for updates...')
    html = requests.get("https://www.rbc.ru/", headers)
    soup = BS(html.text, features='html.parser')
    
    data = soup.find(class_="main__big js-main-reload-item")
    topics = soup.find_all(class_="js-main-reload-item")

    for topic in topics:
        # check if the article is 'pro.rbc'
        if eval(topic.a['data-rm-data-element'])['projectNick'] != 'rbcnews':
            continue
    
        url = topic.a['data-vr-contentbox-url']
    
        modif_date = topic['data-modif-date'] # in 'Sun, 21 Jul 2024 04:42:37 +0300' format 
        modif_date = datetime.strptime(modif_date, "%a, %d %b %Y %X %z")
    
        article_id = topic['data-id']
    
        if not client.exists(index=index, id=article_id):
            print('Creating a new record...')
            record = new_record(url, modif_date, headers)
            client.index(index=index, id=article_id, body=record)
            client.index(index=history_index, id=article_id, body={"history": []})
        elif modif_date > datetime.strptime(client.get_source(index=index, id=article_id, _source_includes="modif_date")['modif_date'], "%Y-%m-%dT%X%z"):
            print('Modifying an existing record...')
            to_history = {}
            old_version = client.get_source(index=index, id=article_id)
            record = new_record(url, modif_date, headers)
            diff = DeepDiff(old_version, record, view='tree')
            if 'dictionary_item_removed' in diff:
                for removed_item in diff['dictionary_item_removed']:
                    removed_key = removed_item.path(output_format='list')[0]
                    to_history[removed_key] = old_version[removed_key]
            if 'dictionary_item_added' in diff:
                for added_item in diff['dictionary_item_added']:
                    added_key = added_item.path(output_format='list')[0]
                    to_history[added_key] = None
            if 'values_changed' in diff:
                for changed_item in diff['values_changed']:
                    changed_key = changed_item.path(output_format='list')[0]
                    to_history[changed_key] = old_version[changed_key]
            to_history['modif_date'] = modif_date
            
            script_body = {
                "script": {
                    "source": "ctx._source.history.add(params)",
                    "params": to_history
                }
            }
            client.update(index=history_index, id=article_id, body=script_body)
        
            client.delete(index=index, id=article_id)
            client.index(index=index, id=article_id, body=record)

    client.close()
    
    print('Done. Waiting for 10 minutes...')
    time.sleep(60 * 10)
