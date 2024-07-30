from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.variable import Variable
from airflow.models import XCom
from airflow.utils.session import create_session

from bs4 import BeautifulSoup 
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta
import boto3
from selenium import webdriver

import ray
import math
import os
import time
from datetime import datetime
import pandas as pd
from typing import List, Tuple
import logging
import os
import glob
import requests
import json
from urllib.parse import quote
from datetime import datetime


from sql import url
from utils import common_util
from utils.constant_util import Directory, Config, Date


###########################################################

def get_soup(url):
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    return soup
    
def get_url(**context):
    shared_list_json = Variable.get("shared_list")
    new_spotify_track_id_list = json.loads(shared_list_json)
    print(f"the number of added new track : {len(new_spotify_track_id_list)}")

    df = pd.DataFrame(context["ti"].xcom_pull(task_ids='select_track_artist_spotify_track_id_task'))
    df = df[df['SPOTIFY_TRACK_ID'].isin(new_spotify_track_id_list)]
    print(f"before : {len(df)}")
    df.drop_duplicates(subset = 'SPOTIFY_TRACK_ID', inplace = True)
    print(f"after : {len(df)}")

    spotify_track_id_list = list(df['SPOTIFY_TRACK_ID'])
    artist_name_list = list(map(lambda x: quote(x), list(df['ARTIST'])))
    track_name_list = list(map(lambda x: quote(x), list(df['TRACK'])))

    basic_url = 'https://www.last.fm/music/'
    
    url_list = []
    for track_name, artist_name in zip(track_name_list, artist_name_list):
        url = basic_url + artist_name.replace(' ', '+') + '/_/' + track_name.replace(' ', '+')
        url_list.append(url)
    
    print(f'the length of url_lst : {len(url_list)}')
    print(f'the length of spotify_track_id_list : {len(spotify_track_id_list)}')
    #print(url_list[0])

    context["ti"].xcom_push(key='url_list', value=url_list)
    context["ti"].xcom_push(key='spotify_track_id_list', value=spotify_track_id_list)


    return url_list

@ray.remote
def get_listeners(url):
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    
    try:
        listeners = soup.select('div.header-new-info-desktop>ul.header-metadata-tnew>li.header-metadata-tnew-item>div.header-metadata-tnew-display')[0].text.strip()
        if listeners[-1] == 'K':
            listeners = float(listeners[:-1]) * 1000
        elif listeners[-1] == 'M':
            listeners = float(listeners[:-1]) * 1000000
        else:
            listeners = float(listeners.replace(',',''))
    except:
        listeners = '0'
    return int(listeners)

@ray.remote
def get_length(url):
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    
    try:
        length = soup.select('div.container.page-content>div.row')[0].find('div', class_='col-main buffer-standard buffer-reset@sm').select('div.metadata-column>dl.catalogue-metadata>dd.catalogue-metadata-description')[0].text.strip()
        length = int(length[0]) * 60 + int(length[2:])
    except:
        length = ''
    return length

@ray.remote
def get_genres(url):
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    
    try:
        group = soup.select('div.container.page-content>div.row')[0].find('div', class_='row buffer-3 buffer-4@sm').select('div.col-sm-8>div.section-with-separator.section-with-separator--xs-only>section.catalogue-tags>ul.tags-list.tags-list--global')[0].find_all('li', class_='tag')
        genres = [genre.text for genre in group]
    except:
        genres = ''
    return genres

@ray.remote
def get_introduction(url):
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    
    try:
        introduction = soup.find_all('div',class_='wiki-content')[0].text.strip()
    except:
        introduction = ''
    return introduction

@ray.remote
def get_reviews(review_url):
    html = requests.get(review_url).text
    soup = BeautifulSoup(html, 'html.parser')
    
    review_collection = []
    # 리뷰 페이지 여러개 일 때
    try:
        pages = int(soup.select('div.col-main>section>section.js-shouts-container.shoutbox.shoutbox--subpage>nav.pagination>ul.pagination-list')[0].find_all('li', class_='pagination-page')[-1].text.strip())
        for page in range(1,pages+1):
            x = review_url + str(page)
            html = requests.get(x).text
            soup = BeautifulSoup(html, 'html.parser')
            reviews = soup.select('div.col-main>section>section.js-shouts-container.shoutbox.shoutbox--subpage>ul.shout-list.js-shout-list')[0]
            for review in reviews.select('li.shout-list-item.js-shout-list-item.js-shouts-container>div.shout-container>div.shout.js-shout.js-link-block'):
                dic = {}
                try:
                    dic['review'] = review.find('div',class_='shout-body').text.strip().replace('\r', ' ').replace('\n', ' ')
                except:
                    dic['review'] = ''
                try:
                    date_str = review.find('a',class_='shout-permalink shout-timestamp').text.strip()
                    datetime_obj = datetime.strptime(date_str, '%d %b %Y, %I:%M%p')
                    dic['date'] = datetime_obj.strftime('%Y-%m-%d %H:%M')
                except:
                    try:    
                        date_str = review.find('a',class_='shout-permalink shout-timestamp').text.strip()
                        datetime_obj = datetime.strptime(date_str, '%d %b %I:%M%p')
                        dic['date'] = str(datetime.now().year)+ '-' +datetime_obj.strftime('%m-%d %H:%M')
                    except:
                        dic['date'] = datetime.now().strftime('%Y-%m-%d %H:%M')
                try:    
                    dic['likes'] = int(review.select('ul.shout-actions>li.shout-action>div.vote-button-toggle')[0].text.strip().split('\n')[-1].strip()[:-6])
                except:
                    dic['likes'] = 0
                review_collection.append(dic)
    except:
            # 리뷰 없음
            try:
                test = soup.select('div.col-main>section>section.js-shouts-container.shoutbox.shoutbox--subpage>p.no-data-message.js-shouts-insertion-hook')[0].text.strip()
                if test[:6] == 'Nobody':
                    review_collection = ''
            # 리뷰 페이지 한 개
            except:
                reviews = soup.select('div.col-main>section>section.js-shouts-container.shoutbox.shoutbox--subpage>ul.shout-list.js-shout-list')[0]
                for review in reviews.select('li.shout-list-item.js-shout-list-item.js-shouts-container>div.shout-container>div.shout.js-shout.js-link-block'):
                    dic = {}
                    try:
                        dic['review'] = review.find('div',class_='shout-body').text.strip().replace('\r', ' ').replace('\n', ' ')
                    except:
                        dic['review'] = ''
                    try:
                        date_str = review.find('a',class_='shout-permalink shout-timestamp').text.strip()
                        datetime_obj = datetime.strptime(date_str, '%d %b %Y, %I:%M%p')
                        dic['date'] = datetime_obj.strftime('%Y-%m-%d %H:%M')
                    except:
                        try:    
                            date_str = review.find('a',class_='shout-permalink shout-timestamp').text.strip()
                            datetime_obj = datetime.strptime(date_str, '%d %b %I:%M%p')
                            dic['date'] = str(datetime.now().year)+ '-' +datetime_obj.strftime('%m-%d %H:%M')
                        except:
                            dic['date'] = datetime.now().strftime('%Y-%m-%d %H:%M')
                    try:
                        dic['likes'] = int(review.select('ul.shout-actions>li.shout-action>div.vote-button-toggle')[0].text.strip().split('\n')[-1].strip()[:-6])
                    except:
                        dic['likes'] = 0
                    review_collection.append(dic)

    return review_collection

###########################################################
# urls => 새로 추가되는 곡만

def get_info(**kwargs):
    ti = kwargs['ti']
    url_list = ti.xcom_pull(key='url_list')
    spotify_track_id_list = ti.xcom_pull(key='spotify_track_id_list')
    print(f"the number of urls : {len(url_list)}")
    print(spotify_track_id_list)

    num_cpus = 8

    # ray 시작
    ray.init(num_cpus = num_cpus, ignore_reinit_error=True)

    results = []
    for url in url_list:
        listener = get_listeners.remote(url)
        length = get_length.remote(url)
        genre = get_genres.remote(url)
    
        wiki_url = url + '/+wiki'
        wiki = get_introduction.remote(wiki_url)
        results.append([listener, length, genre, wiki])

    info_dic = {}
    cnt = 0
    for spotify_track_id, url, result in zip(spotify_track_id_list, url_list, results):
        cnt += 1
        if cnt % 100 == 0:
            print(f"{spotify_track_id} reached at {cnt}")

        info_value_dic = {}
        listener, length, genre, wiki = ray.get(result)
        info_value_dic['listeners'] = listener
        info_value_dic['length'] = length
        info_value_dic['genres'] = genre
        info_value_dic['last_fm_url'] = url
        info_value_dic['introduction'] = wiki
        info_dic[spotify_track_id] = info_value_dic


    logging.info('Getting info successfully!')

    info_src_path = os.path.join(Directory.DOWNLOADS_DIR, f'last_fm/information')
    os.makedirs(info_src_path, exist_ok=True)

    for spotify_id, value in info_dic.items():
        info_json_path = os.path.join(info_src_path, f'{spotify_id}.json')
        
        with open(info_json_path, 'w', encoding='utf-8') as info_json_file:
            json.dump(value, info_json_file, ensure_ascii=False, indent=4)
    logging.info(f'Getting {spotify_id} info successfully!')

    return info_dic

def get_review(**kwargs):
    ti = kwargs['ti']
    url_list = ti.xcom_pull(key='url_list')
    spotify_track_id_list = ti.xcom_pull(key='spotify_track_id_list')
    print(f"the number of url_list : {len(url_list)}")
    print(f"the number of spotify_track_id_list : {len(spotify_track_id_list)}")

    num_cpus = 8

    # ray 시작
    ray.init(num_cpus = num_cpus, ignore_reinit_error=True)

    titles_process = []
    for url in url_list:   
        review_url = url + '/+shoutbox?sort=newest&page='
        info = get_reviews.remote(review_url)
        titles_process.append(info)
    print(f"the number of titles_process : {len(titles_process)}")

    total_review_list = []
    cnt = 0
    for spotify_track_id, title in zip(spotify_track_id_list, titles_process):
        cnt += 1
        if cnt % 100 == 0:
            print(f"{spotify_track_id} reached at {cnt}")

        tmp = {}
        
        try:
            reviews = ray.get(title)
            if reviews != '':
                tmp[spotify_track_id] = reviews
                total_review_list.append(tmp)
            else:
                continue
        except:
            pass
        
        cnt += 1
    
    logging.info('Getting review successfully!')

    print(f'the number of total_review : {len(total_review_list)}')

    if len(total_review_list) != 0:
        reviews_src_path = os.path.join(Directory.DOWNLOADS_DIR, f'last_fm/reviews')
        os.makedirs(reviews_src_path, exist_ok=True)
        logging.info("Getting info start!")

        for review_dict in total_review_list:
            for spotify_id in list(review_dict.keys()):
                reviews_json_path = os.path.join(reviews_src_path, f'{spotify_id}.json')
                if os.path.exists(reviews_json_path):
                    with open(reviews_json_path, 'r', encoding='utf-8') as review_json_file:
                        review_dic_json = json.load(review_json_file)
                    review_dic_json.update(review_dict)

                    with open(reviews_json_path, 'w', encoding='utf-8') as review_json_file:
                        json.dump(review_dic_json, review_json_file, ensure_ascii=False, indent=4)
                else:
                    with open(reviews_json_path, 'w', encoding='utf-8') as review_json_file:
                        json.dump(review_dict, review_json_file, ensure_ascii=False, indent=4)
        logging.info(f'Getting {spotify_id} review successfully!')
    
    return total_review_list


##############################################################


def upload_raw_files_to_s3(bucket_name: str, category: str, **kwargs) -> None:
    shared_list_json = Variable.get("shared_list")
    new_spotify_track_id_list = json.loads(shared_list_json)

    src_path = os.path.join(Directory.DOWNLOADS_DIR, f'last_fm/{category}')
    filenames = [os.path.join(src_path, f"{spotify_id}.json") for spotify_id in new_spotify_track_id_list]
    keys = [filename.replace(Directory.AIRFLOW_HOME, "")[1:] for filename in filenames]
    
    if len(filenames) > 0 :
        logging.info(filenames[0])
        logging.info(keys[0])
    
    common_util.upload_files_to_s3(filenames=filenames, keys=keys, bucket_name=bucket_name, replace=True)

    


def delete_xcoms_for_dag(dag_id, **kwargs):
    with create_session() as session:
        session.query(XCom).filter(
            XCom.dag_id == dag_id
        ).delete(synchronize_session=False)
        session.commit()

NUM_PARTITION = 3
with DAG(dag_id="last_fm_dag",
         schedule_interval="@daily",
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )

    select_track_artist_spotify_track_id_task = SnowflakeOperator(
        task_id="select_track_artist_spotify_track_id_task",
        sql= url.select_track_artist_spotify_track_id,
        snowflake_conn_id='s3_to_snowflake',
        do_xcom_push=True
    )

    get_urls_task  = PythonOperator(
        task_id = "get_urls_task",
        python_callable=get_url,
        provide_context = True
    )
    

    get_info_task  = PythonOperator(
        task_id = "get_info_task",
        python_callable=get_info,
        provide_context = True
    )

    get_review_task  = PythonOperator(
        task_id = "get_review_task",
        python_callable=get_review,
        provide_context = True
    )
    upload_reviews_to_s3_task = PythonOperator(
        task_id = "upload_reviews_to_s3_task",
        python_callable= upload_raw_files_to_s3,
        op_kwargs= {
            "bucket_name": Config.BUCKET_NAME,
            "category" : 'reviews'
        }
    )

    upload_information_to_s3_task = PythonOperator(
        task_id = "upload_information_to_s3_task",
        python_callable= upload_raw_files_to_s3,
        op_kwargs= {
            "bucket_name": Config.BUCKET_NAME,
            "category" : 'information'
        }
    )

    call_trigger_task = TriggerDagRunOperator(
        task_id='call_trigger',
        trigger_dag_id='transform_last_fm_dag',
        trigger_run_id=None,
        execution_date=None,
        reset_dag_run=False,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=None,
    )

    delete_xcom_task = PythonOperator(
        task_id="delete_xcom_task",
        python_callable=delete_xcoms_for_dag,
        op_kwargs={'dag_id': 'last_fm_dag'}
    )

    end_task = EmptyOperator(
        task_id = "end_task"
    )

    start_task >> select_track_artist_spotify_track_id_task >> get_urls_task

    get_urls_task >> get_info_task
    get_urls_task >> get_review_task

    get_info_task >> upload_information_to_s3_task
    get_review_task >> upload_reviews_to_s3_task

    
    upload_information_to_s3_task >> call_trigger_task
    upload_reviews_to_s3_task >> call_trigger_task >> delete_xcom_task >> end_task