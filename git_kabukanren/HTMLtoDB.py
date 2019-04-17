# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import csv
import glob
import os
import sqlite3

def generate_price_from_csv_file(html_name, code):
    f = open(html_name, "r", encoding="utf-8_sig")
    html = f.read()
    f.close()
    soup = BeautifulSoup(html, 'html.parser')

    # 対象銘柄が無かった場合はスキップ
    if len(soup.find_all("table", {"class": "stock_table stock_data_table"})) == 0:
        exit()

    soup.find_all("thead")[0].find("tr").extract()

    tmp1 = soup.find_all("tr")
    cnt = 0

    for p1 in tmp1:
        # 銘柄コード
        for i, data in enumerate(p1):
            if (i == 1):
                # print(data.string) 日付
                d = str(data.string)

            if (i == 3):
                # print(data.string)  # 始値
                o = float(data.string)

            if (i == 5):
                # print(data.string)  # 高値
                h = float(data.string)

            if (i == 7):
                # print(data.string)  # 安値
                l = float(data.string)

            if (i == 9):
                # print(data.string)  # 終値
                c = float(data.string)

            if (i == 11):
                # print(data.string)  # 出来高
                v = float(data.string)

            if (i == 13):
                # print(data.string)  # 終値調整
                z = float(data.string)
                cnt += 1

                yield code, d, o, h, l, c, v,z

def generate_from_csv_dir(html_dir, generate_func):
    for path in glob.glob(os.path.join(html_dir,  "*_*.html")):
        file_name = os.path.basename(path)
        code = file_name.replace('.html','').split('_')[0]
        for d in generate_func(path, code):
            yield d

def all_csv_file_to_db(db_file_name, html_dir):
    price_generator = generate_from_csv_dir(html_dir,
                                            generate_price_from_csv_file)
    conn = sqlite3.connect(db_file_name)
    with conn:
        sql = "INSERT OR REPLACE INTO TMP_BrandsData_R(BrandCode, DataDate, OpenPrice, HighPrice, LowPrice, ClosePrice, Yield, ClosePriceAdjust, RegTime, UpdTime)" \
          "VALUES(?,?,?,?,?,?,?,?,datetime('now', 'localtime'),datetime('now', 'localtime'))"
        conn.executemany(sql, price_generator)

all_csv_file_to_db('stock_col.db', 'C:\Python自習\py_proj\html')