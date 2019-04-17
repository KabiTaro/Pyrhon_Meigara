import pandas as pd
import datetime
import sqlite3

def Get_DataF(data):
  #決算プロ【http://ke.kabupro.jp】の全上場企業・短信XBRL一括ダウンロードページより取得
  df = pd.read_excel(r'http://ke.kabupro.jp/down/{}f.xls'.format(data.strftime("%Y%m%d")))\
      .query('連結個別 == "連結" & 情報公開又は更新日 == "{}"'.format(data.strftime("%Y/%m/%d")))

  CashFlow = df.drop(['企業名', '決算期間', '期首', '会計基準', '連結個別', '決算期', '名寄前勘定科目（売上高欄に掲載）', '売上高',
                      '営業利益', '経常利益', '純利益', '一株当り純利益', '希薄化後一株当り純利益', '純資産又は株主資本',
                      '総資産', '一株当り純資産'], axis=1).dropna(subset=['営業キャッシュフロー', '投資キャッシュフロー', '財務キャッシュフロー'])\
      .drop_duplicates(subset=['証券コード', '期末'], keep='last')

  QtlyRslt = df.drop(['企業名', '期首', '会計基準', '連結個別', '決算期', '名寄前勘定科目（売上高欄に掲載）', '営業キャッシュフロー',
                      '投資キャッシュフロー', '財務キャッシュフロー'], axis=1).drop_duplicates(subset=['証券コード', '決算期間', '期末'], keep='last')

  return QtlyRslt.values.tolist(), CashFlow.values.tolist()

def Row_data_Qtly(row):
    code = row[0]
    name = row[1]
    term = row[2].strftime("%Y-%m-%d")
    amount_sales = row[3]
    ope_income = row[4]
    ord_income = row[5]
    net_income = row[6]
    net_income_per_share = row[7]
    diluted_net_income_per_share = row[8]
    net_assets = row[9]
    total_assets = row[10]
    net_assets_per_share = row[11]
    presendate = row[12].strftime("%Y-%m-%d")

    return code, name, term, amount_sales, ope_income, ord_income, net_income, net_income_per_share, \
           diluted_net_income_per_share, net_assets, total_assets, net_assets_per_share, presendate

def Row_data_CashData(row):
    code = row[0]
    term = row[1].strftime("%Y-%m-%d")
    sales_cash = row[2]
    invest_cash = row[3]
    financial_cash = row[4]
    presendate = row[5].strftime("%Y-%m-%d")

    return code, term, sales_cash, invest_cash, financial_cash, presendate

def QtlyData_Generator(data):
    for index in range(len(data )):
        d = Row_data_Qtly(data[index])
        yield d

def CashData_Generator(data):
    for index in range(len(data )):
        h = Row_data_CashData(data[index])
        yield h

def insert_data_to_db(db_file_name):
  conn = sqlite3.connect(db_file_name)
  now = datetime.datetime.now()
  data = Get_DataF(now)

  #更新が無ければ終了
  if len(data[0]) == 0 and len(data[1]) == 0:
      print('更新無し')
      return None

  with conn:
    conn.execute('BEGIN TRANSACTION')
    sql1 = "INSERT OR REPLACE INTO Corp_QuarterlyResults_R(BrandCode, TermName, Term, Amount_Sales, Ope_Income," \
          "Ord_Income,Net_Income,Net_Income_Per_Share,Diluted_Net_Income_Per_Share,Net_Assets,Total_Assets,"\
          "Net_Assets_Per_Share,PresenDate,RegTime, UpdTime)" \
          "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now', 'localtime'),datetime('now', 'localtime'))"
    conn.executemany(sql1, QtlyData_Generator(data[0]))

    sql2 = "INSERT OR REPLACE INTO Corp_CashFlow_R(BrandCode, Term, Sales_Cash, Invest_Cash, Financial_Cash, PresenDate, RegTime, UpdTime)" \
          "VALUES(?,?,?,?,?,?,datetime('now', 'localtime'),datetime('now', 'localtime'))"
    conn.executemany(sql2, CashData_Generator(data[1]))
    conn.commit()

#現在の日時をyyyymmdd形式で取得
insert_data_to_db(r"stock_col.db")