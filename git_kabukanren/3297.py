import pandas as pd
import pandas_datareader.data as web
import pandas_highcharts
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import mpl_finance
import sqlite3
from pandas_highcharts.display import display_charts

def Get_data_from_DB(db_file_name,codes):
    conn = sqlite3.connect(db_file_name)
    return pd.read_sql_query("SELECT * FROM TMP_BrandsData_R WHERE `BrandCode` = ?", conn,params=(codes,),index_col='DataDate')

#df = pd.read_csv('TMP_BrandsData_R.csv',sep=',').query('BrandCode == 1434')

df = Get_data_from_DB("stock_col.db",'3297')

# df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
# df['ClosePrice'].plot()
# plt.show()
# print(df.index .year)
#
# df_ = df[['OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice','Yield']].copy()
# df_.index = mdates.date2num(df_.index)
#
# data = df_.reset_index().values
# print(data)
#
# fig = plt.figure(figsize=(12, 4))
# ax = fig.add_subplot(1, 1, 1)
#
# mpl_finance.candlestick_ohlc(ax, data, width=1, alpha=1, colorup='r', colordown='b')
#
# ax.grid()
#
# ax.xaxis.set_major_locator(mdates.MonthLocator())
# ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y/%m'))
# plt.show()