# -*- coding: utf-8 -*-
import csv
import glob
import datetime
import os
import sqlite3


def generate_price_from_csv_file(csv_file_name):
    with open(csv_file_name) as f:
        reader = csv.reader(f)
        for row in reader:
            code = str(row[0])
            d = str(row[1]) #日付
            o = float(row[2])  # 初値
            h = float(row[3])  # 高値
            l = float(row[4])  # 安値
            c = float(row[5])  # 終値
            v = float(row[6])    # 出来高
            z = float(row[7])    # 終値最終調整
            mo = str(row[8])  # 終値最終調整
            me = str(row[9])  # 終値最終調整
            yield code, d, o, h, l, c, v,z,mo,me


def generate_from_csv_dir(csv_dir, generate_func):
    for path in glob.glob(os.path.join(csv_dir, "*.csv")):
        for d in generate_func(path):
            yield d


def all_csv_file_to_db(db_file_name, csv_file_dir):
    price_generator = generate_from_csv_dir(csv_file_dir,
                                            generate_price_from_csv_file)
    conn = sqlite3.connect(db_file_name)
    with conn:
        sql = "INSERT OR REPLACE INTO TMP_BrandsData_R(BrandCode, DataDate, OpenPrice, HighPrice, LowPrice, ClosePrice, Yield, ClosePriceAdjust, RegTime, UpdTime)" \
          "VALUES(?,?,?,?,?,?,?,?,?,?)"
        conn.executemany(sql, price_generator)

all_csv_file_to_db('stock_col.db', r'C:\Python自習\py_proj\backup')