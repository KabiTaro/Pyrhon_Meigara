from pyquery import PyQuery

q = PyQuery('https://www.traders.co.jp/stocks_info/individual_info_basic.asp?SC=7743')
sector = q.find('table.kobetsu_data_table2 a')[0].text
print(sector)