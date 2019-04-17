# -*- coding: utf-8 -*-
import datetime
import sqlite3
import pandas as pd

def generater_devide_union_data(data,index):
    try:
        def parse_recode(row):
            d = datetime.datetime.strptime(row[1], '%Y-%m-%d').date() #日付
            r = float(row[5])  # 調整前終値
            a = float(row[7])  # 調整後終値
            return d, r, a
        if(data[index][0] == data[index+1][0]): #処理対象コードが合致する場合のみ
            _, r_n, a_n = parse_recode(data[index+1])
            d, r, a = parse_recode(data[index])
            if((a_n * r) != 0 and (a * r_n) != 0 ):
                rate = (a_n * r) / (a * r_n)
                if abs(rate - 1) > 0.005:
                    if rate < 1:
                        before = round(1 / rate, 1)
                        after = 1
                    else:
                        before = 1
                        after = round(rate, 1)
                    if int(before) != int(after) :
                        yield data[index][0], d, int(before), int(after)
            r_n = r
            a_n = a
    except IndexError as err:
        print('cannot open', err)

def generate_from_data(data,generate_func):
    for index in range(len(data)):
        if (index == len(data)-1):
            break
        for d in generate_func(data,index):
            yield d

def all_csv_file_to_divide_union_table(db_file_name):
    conn = sqlite3.connect(db_file_name)
    data = pd.read_sql_query("SELECT * FROM TMP_BrandsData_R", conn).values.tolist()
    divide_union_generator = generate_from_data(data,generater_devide_union_data)
    with conn:
        sql = """
        INSERT OR REPLACE INTO DivideUnionData_R(BrandCode,RightAllomentDate,TngnStock_before, TngnStock_after,RegTime,UpdTime)
        VALUES(?,?,?,?,datetime('now', 'localtime'),datetime('now', 'localtime'))
        """
        conn.executemany(sql, divide_union_generator)

all_csv_file_to_divide_union_table('stock_col.db')