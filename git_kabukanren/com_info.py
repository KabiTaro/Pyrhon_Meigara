# -*- coding: utf-8 -*-
import time
import sqlite3
from bs4 import BeautifulSoup
import requests
import random
import _proxy

def get_brand(BrandCode):
  soup = BeautifulSoup(requests.get('https://kabutan.jp/stock/?code={}'.format(BrandCode), proxies=_proxy.proxies).content, 'html.parser')

  if len(soup.find_all("div", {"id": "kobetsu_left"})) == 0:
    return None

  try:
    #会社名
    KsyaName = soup.find_all("h4")[0].string.strip()

    #会社略称
    KsyaRyakName = soup.find_all("table",{"class":"kobetsu_data_table1"})[0].find_all("table")[0].find_all("td")[1].text

    # 上場市場
    JojoMket= soup.find_all("table",{"class":"kobetsu_data_table1"})[0].find_all("table")[0].find_all("td")[2].text

    #株単位
    TngnStock_num = soup.find_all("table",{"class":"kobetsu_data_table2"})[0].find_all("tr")[2].find_all("td")[0].string.replace('単位', '').replace('株', '').strip()

    #会社アドレス
    Url = soup.find_all("div",{"id":"kobetsu_right"})[0].find_all("dd")[0].text

    #概要
    Gaiyo = soup.find_all("div",{"id":"kobetsu_right"})[0].find_all("dd")[1].text

    #会社分類
    Sector = soup.find_all("div",{"id":"kobetsu_right"})[0].find_all("dd")[2].text

  except ValueError:
    return None

  print(KsyaName)
  return BrandCode, KsyaName, KsyaRyakName, JojoMket, Url, Sector, Gaiyo, TngnStock_num

def brands_generator(code_range):
  for code in code_range:
    time.sleep(random.randrange(2,5))
    brand = get_brand(code)
    if brand:
      yield brand


def insert_brands_to_db(db_file_name, code_range):
  conn = sqlite3.connect(db_file_name)
  with conn:
    sql = "INSERT INTO Brand_R(BrandCode, KsyaName, KsyaRyakName,JojoMket, Url, Sector, Gaiyo, TngnStock_num, RegTime, UpdTime)" \
          "VALUES(?,?,?,?,?,?,?,?,datetime('now', 'localtime'),datetime('now', 'localtime'))"
    conn.executemany(sql, brands_generator(code_range))

insert_brands_to_db("stock_col.db", range(7600,9998))