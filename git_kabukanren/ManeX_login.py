from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime, date, timedelta
import time
from selenium import webdriver

#ログイン
driver = webdriver.Chrome()
driver.get('https://mst.monex.co.jp/pc/ITS/login/LoginIDPassword.jsp')
time.sleep(2.5)

# ID/PASSを入力
id = driver.find_element_by_id("loginid")
id.send_keys("77507213")

password = driver.find_element_by_id("passwd")
password.send_keys("aaaaa")

time.sleep(1)

# ログインボタンをクリック
login_button = driver.find_element_by_css_selector('input.text-button')
login_button.click()

time.sleep(5)

driver.quit()