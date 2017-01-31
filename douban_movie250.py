# __author__ = 'c1rew'
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import re
import time
import pymysql.cursors

header = {'User-Agent':
              'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36'}

urls = ['https://movie.douban.com/top250?start={}&filter='.format(str(i)) for i in range(0, 250, 25)]


def parse_one_movie(infos):
    url = infos.find("a", href=re.compile(r"https://movie\.douban\.com/subject/[0-9]+/"))['href']
    titles = infos.findAll("span", class_="title")
    title = ""
    if isinstance(titles, list):
        for item in titles:
            title = title + item.get_text()
    else:
        title = titles.get_text()
    other_title = infos.find("span", class_="other").string[3:] # delete / and space
    rating_num = infos.find("span", class_="rating_num").string
    movie_info = infos.find("div", class_="bd").p.get_text().strip()
    movie_infos = movie_info.split('\n')
    director_actor = movie_infos[0].strip()
    other_infos = movie_infos[1].strip().split('/')
    year = other_infos[0].strip()
    region = other_infos[1].strip()
    movie_type = other_infos[2].strip()
    introduce = infos.find("span", class_="inq").string


def get_url_info(url):
    url_info = requests.get(url, header)
    content = url_info.content.decode('utf-8')
    soup = BeautifulSoup(content, "html.parser")
    items = soup.findAll("div", class_="item")

    for item in items:
        parse_one_movie(item)
    #print(items)
    #time.sleep(1)

sql_create = "CREATE TABLE IF NOT EXISTS douban_top_movie " \
      "( ID INT NOT NULL AUTO_INCREMENT, " \
      " PRIMARY KEY(ID), " \
      " title VARCHAR(128), " \
      " other_title VARCHAR(128), " \
      " url VARCHAR(512), " \
      " rating_num VARCHAR(64), " \
      " director_actor VARCHAR(1024), " \
      " year INT(4), " \
      " region VARCHAR(128), " \
      " movie_type VARCHAR(128), " \
      " introduce VARCHAR(1024)" \
      ");"

# for url in urls:
#     get_url_info(url)

connection = pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             db='douban_infos_db',
                             charset='utf8mb4')
try:
    with connection.cursor() as cursor:
        sql_insert = "insert into `douban_top_movie` (" \
        "`title`, " \
        "`other_title`," \
        "`url`," \
        "`rating_num`," \
        "`director_actor`," \
        "`year`," \
        "`region`," \
        "`movie_type`," \
        "`introduce`" \
        ") values(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(sql_create)

        connection.commit()
finally:
    connection.close()

#get_url_info("https://movie.douban.com/top250?start=0&filter=")
