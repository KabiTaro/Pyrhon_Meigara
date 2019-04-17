# -*- coding: utf-8 -*-
import time
import sqlite3
import random
import _proxy
from bs4 import BeautifulSoup
import zenhan
import requests
import _proxy


soup = BeautifulSoup(requests.get("https://www.jpx.co.jp/listing/stocks/new/index.html", proxies=_proxy.proxies).content,'html.parser')
#テーブルを指定

tmp1 = soup.find_all("table",{"class":"fix-header"})[0]

data={'code': {},'name': {}, 'url': {}, 'jojodate': {}, 'jojosyonindate': {}, 'JoJomrkt': {}}

i = 0
for p1 in tmp1.find_all("td", {"class": "a-center tb-color001"}):
    for p2 in p1.find_all("span"):
        # 銘柄コード
        #print(p2.attrs['id'])
        data['code'][i] = p2.attrs['id']
        i += 1

for i,p1 in enumerate(tmp1.find_all(attrs={"rowspan":"2", "class":"a-center tb-color001 w-space"})):
    #上場日
    data['jojodate'][i] = zenhan.z2h(p1.contents[0].string).strip()

i = 0
for p1 in tmp1.find_all(attrs={"rowspan":"2", "class":"a-left tb-color001"}):

    #会社名
    data['name'][i] = p1.find("a").text.replace('*', '').strip()

    i += 1

i = 0
for p1 in tmp1.find_all("tr"):
    for p2 in p1.find_all("td",{"class": "a-center tb-color001"}):
        for p3 in p2:
            if ("第一部" in p3  or "第二部" in p3 or "マザーズ" in p3 or "JQスタンダード" in p3) :
                #print(p3.replace('第一部','東証１').replace('第二部','東証２').replace('マザーズ','東証Ｍ').replace('JQスタンダード','ＪＱ'))
                data['JoJomrkt'][i] = p3.replace('第一部','東証１').replace('第二部','東証２').replace('マザーズ','東証Ｍ').replace('JQスタンダード','ＪＱ')
                i += 1

def get_data(int):
    BrandCode = data['code'][int]
    KsyaName = data['name'][int]
    JoJoMrkt = data['JoJomrkt'][int]
    JoJoDate = data['jojodate'][int]
    return BrandCode, KsyaName, JoJoMrkt, JoJoDate,

def data_generator(int_range):
    for mun in int_range:
        dn = get_data(mun)
        if dn :
            yield dn

def insert_data_to_db(db_file_name):
  conn = sqlite3.connect(db_file_name)
  lim =len(data['code'])
  print(data)
  with conn:
    conn.execute('BEGIN TRANSACTION')
    sql = "INSERT OR REPLACE INTO CorpAct_JoJoBrands_R(BrandCode, KsyaName, JoJoMrkt, JoJoDate, RegTime, UpdTime)" \
          "VALUES(?,?,?,?,datetime('now', 'localtime'),datetime('now', 'localtime'))"
    conn.executemany(sql, data_generator(range(1, lim)))

insert_data_to_db("stock_col.db")

