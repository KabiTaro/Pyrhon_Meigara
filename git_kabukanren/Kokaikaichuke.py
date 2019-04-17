import  sys
from bs4 import BeautifulSoup
import csv
import glob
import os
import sqlite3, _proxy , requests , datetime , time

#公開買付 取得
def TOB_generate_GetData(soup):
    # 対象銘柄が無かった場合はスキップ
    if len(soup.find_all("table", {"class": "table table-style01 type-standard mb-20"})) == 0:exit()

    soup.find("table").find("tr").extract()

    for data in soup.find_all("tr"):
        code = data.find_all("td")[0].string.split('(')[1].replace(')', '') #銘柄コード
        brandname = data.find_all("td")[0].string.split('(')[0] #被公開買付銘柄名
        price = data.find_all("td")[1].string #買付価格
        TOBbuy = data.find_all("td")[2].get_text().split('(')[0].strip() #公開買付期間
        TOBman = data.find_all("td")[3].string #公開買付者
        TOBdairi = data.find_all("td")[4].string  # 公開買付代理人
        biko = data.find_all("td")[5].string.replace(' ', '').strip()  #備考

        yield code, brandname, price, TOBbuy, TOBman, TOBdairi, biko

def TOB_generate_hiashi(int_range, generate_func):
    for int in int_range:
        for d in generate_func(get_html(int)):
            yield d

def TOB(db_file_name, int_range):
    price_generator = TOB_generate_hiashi(int_range, TOB_generate_GetData)
    conn = sqlite3.connect(db_file_name)
    with conn:
        sql = "INSERT OR REPLACE INTO CorpAct_TOB_R(BrandCode, BrandName, Price, TOBDate, TOBMan , TOBDairi, Biko, RegTime, UpdTime)" \
          "VALUES(?,?,?,?,?,?,?,datetime('now', 'localtime'),datetime('now', 'localtime'))"
        conn.executemany(sql, price_generator)

#公開買付 取得
def get_html(int):
    html = requests.get('https://mst.monex.co.jp/pc/servlet/ITS/info/Tob?searchYear='+str(int), proxies=_proxy.proxies).content
    soup = BeautifulSoup(html, 'html.parser')
    time.sleep(5)
    return soup

TOB('stock_col.db', range(2006, 2018))