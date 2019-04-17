from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime, date, timedelta
import time
from selenium import webdriver


#DBから特定の銘柄データを取得
def Get_data_from_DB(conn, codes):
    return pd.read_sql_query("SELECT * FROM TMP_BrandsData_R WHERE `BrandCode` = ?", conn,params=(codes,),index_col='DataDate')

#移動平均を取得
def avarage_get(conn, code):
    # date_of_right_allotment 以前の分割・併合データで未適用のものを取得する
    df = Get_data_from_DB(conn, code)

    moave5 = df['ClosePriceAdjust'].rolling(5).mean().tolist()
    moave14 = df['ClosePriceAdjust'].rolling(14).mean().tolist()
    moave25 = df['ClosePriceAdjust'].rolling(25).mean().tolist()

    i = 0
    for index, row in df.iterrows():
        conn.execute("INSERT OR REPLACE INTO HoldBrand_MovingPrice"
                     "(BrandCode, DataDate, MovingAverage5, MovingAverage14, MovingAverage25, RegTime, UpdTime)"
                     "VALUES(?, ?, ?, ?, ?, datetime('now', 'localtime'),datetime('now', 'localtime'))",
                     (row.BrandCode, index, round(moave5[i], 2), round(moave14[i], 2), round(moave25[i], 2)))
        i += 1

    conn.commit()
        # print(df['ClosePriceAdjust'].rolling(5).mean())
        # print(df['ClosePriceAdjust'].rolling(14).mean())
        # print(df['ClosePriceAdjust'].rolling(25).mean())

    # with conn:
    #     for BrandCode, DataDate, ClosePriceAdjust in TMP_BrandsData_R:
    #         #移動平均線は、通常、過去ｎ日（週・月）間と設定された日（週・月）の終値の平均値を表示しています。
    #         # ◆株価移動平均（25日）　表示日9月15日
    #         # 　　9月15日をふくめた過去25営業日の各日の終値合計を、25で割る。
    #         # ◆株価移動平均（26週）　表示日9月15日9月15日の週を1週とし、この週を含めた過去26週の各週の終値合計を、
    #         # 26で割る。
    #         # ◆株価移動平均（24月）　表示日9月15日
    #         # 9月を1月とし、この月を含めた過去24ヶ月の各月の終値合計を、24で割る。




#HTMLから株価を取得
def generate_price_from_csv_file(code,date):

    soup = BeautifulSoup(get_html(code, date), 'html.parser')

    # 対象銘柄が無かった場合はスキップ
    if len(soup.find_all("table", {"class": "stock_table stock_data_table"})) == 0:
        exit()

    soup.find_all("thead")[0].find("tr").extract()

    for data in soup.find_all("tr"):
        d = data.find_all("td")[0].string
        o = data.find_all("td")[1].string
        h = data.find_all("td")[2].string
        l = data.find_all("td")[3].string
        c = data.find_all("td")[4].string
        p = data.find_all("td")[5].string
        x = data.find_all("td")[6].string

        yield code, d, o, h, l, c, p, x

def generate_from_csv_dir(year, code, generate_func, year_lis):
    for yn in range(1994, year+1):

        #既にDBに過去分の日足が存在するならアクセスしない
        if yn in year_lis : continue

        if(yn == year+1): break

        for d in generate_func(code, yn):
            yield d

def all_csv_file_to_db(db_file_name,code):

    now_year = datetime.now().year

    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    conn = sqlite3.connect(db_file_name)

    trgt_lis = exist_year(pd.read_sql_query('SELECT DataDate FROM TMP_BrandsData_R Where BrandCode = ?', conn, params=[code]).values.tolist(), now_year)

    #昨日のデータを取得済みなら処理を中断
    if yesterday in trgt_lis[1]:
        print('最新{}のデータは取得済み'.format(yesterday))
        return None

    with conn:
        sql = "INSERT OR REPLACE INTO TMP_BrandsData_R(BrandCode, DataDate, OpenPrice, HighPrice, LowPrice, ClosePrice, Yield, ClosePriceAdjust, RegTime, UpdTime)" \
          "VALUES(?,?,?,?,?,?,?,?,datetime('now', 'localtime'),datetime('now', 'localtime'))"
        conn.executemany(sql, generate_from_csv_dir(now_year, code, generate_price_from_csv_file,trgt_lis[0]))
        avarage_get(conn, code)

def get_html(code,date):
    driver.get('https://kabuoji3.com/stock/{}/{}/'.format(code, date))
    time.sleep(2.5)
    html = driver.page_source
    return html

#DBに既にある年データを取得する
#実行した年以下のデータは1年分入っている前提＾＾；
def exist_year(year_list, trgt_yaer):
    trgt_year = []
    now_year = []
    for i in year_list:
        nun = datetime.strptime(i[0], '%Y-%m-%d').year
        if not nun in trgt_year and not nun == trgt_yaer:
            trgt_year.append(nun)

        #処理年の日足
        if nun == trgt_yaer:
            now_year.append(i[0])
    return trgt_year, now_year

driver = webdriver.Chrome()
all_csv_file_to_db('stock_col.db', '2914')
driver.quit()