from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
import datetime

dt_now = datetime.datetime.now()
print(dt_now)
# 2018-02-02 18:31:13.271231

print(type(dt_now))
# <class 'datetime.datetime'>

print(dt_now.year)
# 2018

print(dt_now.hour)
# 18