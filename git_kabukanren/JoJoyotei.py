# -*- coding: utf-8 -*-
import time
import sqlite3
import random
from bs4 import BeautifulSoup
import zenhan
import requests
import _proxy

soup = BeautifulSoup(requests.get("https://www.jpx.co.jp/listing/stocks/delisted/archives-10.html", proxies=_proxy.proxies).content,'html.parser')
#テーブルを指定

tmp1 = soup.find_all("table",{"class":"fix-header"})[0].find_all("tbody")[0]

data = {'BrandCode': {}, 'DelDate': {}, 'BrandName': {}, 'JoJoMrkt': {}, 'DelRiyu': {}}

for m,p1 in enumerate(tmp1.find_all("tr")):
    for i,n in enumerate(p1):
        if (i == 5):
            data['BrandCode'][m] = p1.contents[5].string
        if (i == 1):
            data['DelDate'][m] = p1.contents[1].string
        if (i == 3):
            data['BrandName'][m] = p1.contents[3].string
        if(i == 7):
            data['JoJoMrkt'][m] = p1.contents[7].string
        if(i == 9):
            data['DelRiyu'][m]= p1.contents[9].string
print(data)

def get_data(i):
    BrandCode = data['BrandCode'][i]
    DelDate = data['DelDate'][i]
    BrandName = data['BrandName'][i]
    JoJoMrkt = data['JoJoMrkt'][i]
    DelRiyu= data['DelRiyu'][i]

    return BrandCode,DelDate,BrandName, JoJoMrkt,DelRiyu

def data_generator(int_range):
    for mun in int_range:
        dn = get_data(mun)
        if dn :
            yield dn

def insert_data_to_db(db_file_name):
  conn = sqlite3.connect(db_file_name)
  lim =len(data['BrandCode'])
  with conn:
    sql = "INSERT OR REPLACE INTO Del_Brands_R(BrandCode, DelDate, BrandName,JoJoMrkt, DelRiyu,RegTime, UpdTime)" \
          "VALUES(?,?,?,?,?,datetime('now', 'localtime'),datetime('now', 'localtime'))"
    conn.executemany(sql, data_generator(range(1, lim)))

insert_data_to_db("stock_col.db")

