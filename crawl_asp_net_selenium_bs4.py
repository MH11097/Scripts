from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys

from bs4 import BeautifulSoup
import re
import pandas as pd
from tabulate import tabulate
import os

# launch url
url = "https://vast.gov.vn/web/guest/van-ban"

# create a new browser session
driver = webdriver.Chrome(ChromeDriverManager().install())
driver.implicitly_wait(30)
driver.get(url)

# Selenium hands the page source to Beautiful Soup
links = BeautifulSoup(driver.page_source, 'lxml')

datalist = []  # empty list
x = 0  # counter
count = len(links.find_all(
    'a', style="padding-top:5px;padding-bottom:5px;font: arial;font-size: 12px;"))
# Beautiful Soup finds all links on the agency page and the loop begins
while x < 100:
    for i in range(count):

        # Selenium visits each Job Title page

        driver.find_elements_by_xpath(
            '//a[@style="text-align: left;"]')[i].click()  # click link

        get_source = BeautifulSoup(driver.page_source, 'lxml')
        table = get_source.find_all('table')[0]

        # Giving the HTML table to pandas to put in a dataframe object
        df = pd.read_html(str(table), header=0)

        # Store the dataframe in a list
        datalist.append(df[0])

        # Ask Selenium to click the back button
        driver.execute_script("window.history.go(-1)")
        x += 1
    try:
        button_next = driver.find_element_by_xpath('//a[@title="nextPage"]')
        button_next.click()

    except:
        break
    # end loop block

# loop has completed

# end the Selenium browser session
driver.quit()

# combine all pandas dataframes in the list into one big dataframe
result = pd.concat([pd.DataFrame(datalist[i])
                   for i in range(len(datalist))], ignore_index=True)


export_csv = result.to_csv(
    r'C:\Users\NC PC\OneDrive\Desktop\result.csv', index=None, header=False)
print('Done')