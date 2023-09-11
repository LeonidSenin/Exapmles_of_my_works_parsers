"""
Цены на арматуру рифленаую 16 (2ф) А500С ГОСТ 34028-2016. Данные еженедельные (Сайт Евраз)

"""
# Импорт необходимых библиотек
import pandas as pd
from bs4 import BeautifulSoup
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from seleniumwire import webdriver
import time

pd.options.mode.chained_assignment = None
pd.set_option('display.max_rows', 550)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('max_colwidth', 70)


# Блок обьявления функции
def parse():
    date_today = datetime.now().date()
    url = 'https://evraz.market/metalloprokat/armatura/armatura_riflenaya/armatura_16_2f_a500s_gost_34028_2016/'
    # options = webdriver.ChromeOptions()
    # options.add_argument("user-agent=Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0) Gecko/20100101 Firefox/84.0")
    # driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver = webdriver.Chrome()
    driver.implicitly_wait(30)
    driver.get(url)
    element = driver.find_element(By.ID, "tons_nav_item")  # ищем элемент по id
    driver.execute_script("arguments[0].scrollIntoView(true);", element)  # скроллим до element
    time.sleep(3)
    element.click()  # кликаем
    # Забираем цену за товар с сайта и преобразуем
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    full = soup.find('span', class_='cart__main-price').text.strip()
    full = re.sub(r'\s+', ' ', full).replace('₽/т', '').replace(' ', '')
    df = pd.DataFrame(columns=['indicator_value', 'period', 'year', 'week'])
    df.loc[len(df.index)] = [float(full), pd.to_datetime(date_today), int(date_today.year),
                             int(date_today.isocalendar()[1])]
    print(df)
    time.sleep(3)
    driver.quit()

if __name__ == '__main__':
    parse()
