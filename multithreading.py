
from os import link
from typing import Text
import selenium
from selenium import webdriver
import threading
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import numpy as np
import csv
import time
from time import sleep
import re
from tabulate import tabulate

t0 = time.time()
csv_file = pd.read_csv(r'C:\Users\NC PC\OneDrive\Desktop\Tiktok.csv',header=0)
data = []

driver = webdriver.Chrome(ChromeDriverManager().install())
linkk = ['https://www.tiktok.com/@lamhuongreview']

for url in linkk:
    driver.get(url)
    sleep(5)
    name = driver.find_element_by_xpath('/html/body/div/div/div[2]/div[2]/div/header/div[1]/div[2]/h2/text()')
    
    follow = driver.find_element_by_xpath('/html/body/div/div/div[2]/div[2]/div/header/h2[1]/div[2]/strong')
    print(name, url, follow)
for j in range(41):
    url = "https://tinhuytthue.vn//van-ban/&cpage="+str(j+1)
    print("click")

    driver.get(url)
    for i in range(10):
        record1 = driver.find_element_by_xpath(
            '//*[@id="center_vanban"]/article/table/tbody/tr['+str(i+2)+']/td[1]').text
        record2 = driver.find_element_by_xpath(
            '//*[@id="center_vanban"]/article/table/tbody/tr['+str(i+2)+']/td[3]').text
        print(record1, record2)
        data.append([record1, record2])


df = pd.DataFrame(data)
df.to_csv('success.csv', index=None, header=False, encoding='utf-8-sig')
t1 = time.time()
print(f"{t1-t0} seconds to run")
