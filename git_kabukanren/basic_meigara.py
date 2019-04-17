# -*- coding: utf-8 -*-
from pyquery import PyQuery
import time, sqlite3, datetime
import requests
import _proxy


def get_brand(BrandCode):
  url = requests.get('https://kabutan.jp/stock/?code={}'.format(BrandCode), proxies=_proxy.proxies).content
  q = PyQuery(url)

  if len(q.find('.stock_st_table')) == 0:
    return None

  try:
    KsyaName = q.find('#kobetsu_right > h4')[0].text
    KsyaRyakName = q.find('td.kobetsu_data_table1_meigara')[0].text
    JojoMket= q.find('td.kobetsu_data_table1_meigara + td')[0].text
    unit_str = q.find('.stock_st_table:eq(1) > tr:eq(5) > td.tar:eq(0)')[0].text
    TngnStock_num = int(unit_str.split()[0].replace(',', ''))
    Sector = q.find('.kobetsu_data_table2 a')[0].text
  except ValueError:
    return None

  print(BrandCode)
  return BrandCode,KsyaName, KsyaRyakName, JojoMket,TngnStock_num , Sector

def brands_generator(code_range):
  for code in code_range:
    brand = get_brand(code)
    if brand:
      yield brand
    time.sleep(1)

def insert_brands_to_db(db_file_name, code_range):
  conn = sqlite3.connect(db_file_name)
  now = datetime.datetime.now()
  with conn:
    sql = 'INSERT INTO Brand_R(BrandCode,KsyaName,KsyaRyakName,JojoMket,Sector,TngnStock_num,RegTime,UpdTime)' \
          'VALUES(?,?,?,?,?,?,'+str(now) + ',' + str(now) + ')'
    conn.executemany(sql, brands_generator(code_range))

insert_brands_to_db("stock_col.db", range(1301, 9998))