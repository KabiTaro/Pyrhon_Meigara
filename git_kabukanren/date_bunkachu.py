# -*- coding: utf-8 -*-
import datetime
import sqlite3
import pandas as pd

def apply_divide_union_data (conn ,RightAllomentDate):
    # date_of_right_allotment 以前の分割・併合データで未適用のものを取得する
    sql = """
    SELECT 
        BrandCode, date(RightAllomentDate), TngnStock_before, TngnStock_after
    FROM 
       DivideUnionData_R
    WHERE
        date(RightAllomentDate) < date(?)
    ORDER BY 
        RightAllomentDate
    """
    cur = conn.execute(sql,[RightAllomentDate,])
    DivideUnionData_R = cur.fetchall()

    with conn:
        conn.execute('BEGIN TRANSACTION')
        for BrandCode, RightAllomentDate, TngnStock_before, TngnStock_after in DivideUnionData_R:
            
            rate = TngnStock_before / TngnStock_after
            inv_rate = 1 / rate

            #データに分割処理を適用
            conn.execute(
              'UPDATE TMP_BrandsData_R SET '
              ' OpenPrice = round(OpenPrice * :rate,2), '
              ' HighPrice = round(HighPrice * :rate,2), '
              ' LowPrice = round(LowPrice  * :rate,2), '
              ' ClosePrice = round(ClosePrice * :rate,2), '
              ' Yield = Yield * :inv_rate,'
              " UpdTime = datetime('now', 'localtime')"
              'WHERE BrandCode = :BrandCode'
              '  AND DataDate <= :RightAllomentDate',
              {'BrandCode' : BrandCode,
               'RightAllomentDate' :RightAllomentDate,
               'rate' : rate,
               'inv_rate' : inv_rate
               })

            #適用Flgを有効化する
            conn.execute(
              'UPDATE DivideUnionData_R SET '
              ' AppFlg = 1, '
              " UpdTime = datetime('now', 'localtime')"
              ' WHERE BrandCode = ?'
              ' AND RightAllomentDate =  ?',
              (BrandCode, RightAllomentDate))

def get_unique_list(seq):
    seen = []
    return [x for x in seq if x not in seen and not seen.append(x)]

def getdate(db_file_name):
    conn = sqlite3.connect(db_file_name)
    data_uniq = get_unique_list(pd.read_sql_query("SELECT RightAllomentDate FROM DivideUnionData_R", conn).values.tolist())
    for index in range(len(data_uniq )):
        if (index == len(data_uniq )-1):
            break
        apply_divide_union_data(conn, data_uniq[index][0])

getdate("stock_col.db")

