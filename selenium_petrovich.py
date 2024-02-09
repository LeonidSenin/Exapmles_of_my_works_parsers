import pandas as pd
import os
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np
import re
# from selenium import webdriver
from selenium.webdriver.common.by import By
from seleniumwire import webdriver
import urllib3

options = webdriver.ChromeOptions()
# webdriver.Chrome()
options.add_argument('ignore-certificate-errors')
options.add_argument('--no-sandbox')
options.add_argument("user-agent=Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0) Gecko/20100101 Firefox/84.0")
options.add_argument("--headless")
options.add_argument('--disable-dev-shm-usage')
options.add_argument("--disable-gpu")

urllib3.disable_warnings()

pd.options.mode.chained_assignment = None
pd.set_option('display.max_rows', 550)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('max_colwidth', 70)

# Блок иморта глобальных переменных
# Переменные для dag
engine = create_engine('postgresql://l_senin:dGiSMKNbae0774j3hG@10.220.75.48:15432/ac_data')
table_name = 'prmn_027_building_material_prices_petrovich_w'
schema_name = 'raw_data'
period = pd.to_datetime(datetime.now().date())  # дата
month_str = {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август',
             9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'}

df_to_db = pd.DataFrame(columns=['indicator', 'region', 'data_source', 'measure_unit', 'periodity', 'indicator_value'])

def get_links(code, city):
    url = f"https://{city}.petrovich.ru/catalog/9576106/{code}/" # ссылка на товар
    driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(25)
    # driver.get(url)
    for request in driver.requests:
        if request.response:
            try:
                price = \
                    driver.find_element(By.CLASS_NAME, 'price-details').text.replace('₽', '').replace('По карте', '').replace(
                        '\n',' ').strip().split()[0] # Цена товара
                measure_unit = driver.find_element(By.CLASS_NAME, 'units-tabs').text.replace('\n', ' ').strip() #
            except:
                price = np.nan
                measure_unit = '-'
            region = city
            name_product = driver.find_element(By.CLASS_NAME, 'product-title').text.split('\n')[-1] # имя товара
            print(f'Имя товара: {name_product} \nЦена: {price} \nРегион: {region}\nЕдиница измерения: {measure_unit}')
            driver.quit()
            df_row = pd.DataFrame(
                {'indicator': ['Цена на ' + name_product], 'region': [region],
                 'data_source': 'Сайт Строительный Торговый Дом Петрович', 'measure_unit': [measure_unit],
                 'periodity': 'Еженедельно', 'indicator_value': price})  # создаем датафрейм из одной строки
            global df_to_db
            df_to_db = pd.concat([df_to_db, df_row], ignore_index=True)

if __name__ == '__main__':
    list_code = [109624,
                 109621,
                 105702,
                 656419,
                 142571,
                 100986,
                 100834,
                 630234,
                 681119,
                 981197,
                 137076,
                 102326,
                 638362,
                 503760,
                 13440]  # код товара
    city_name = ['moscow', 'arkhangelsk', 'astrakhan', 'novgorod', 'vladimir', 'volzhskiy', 'vyborg',
                 'gatchina', 'gubkin', 'zheleznogorsk', 'i-ola', 'kazan', 'kaluga', 'kingisepp', 'kirov', 'kursk',
                 'lipetsk', 'luga', 'magnitogorsk', 'naberezhnye', 'nizhnevartovsk', 'nizhniy-novgorod',
                 'nizhniy-tagil', 'orel', 'pervouralsk', 'petrozavodsk', 'pskov', 'ryazan', 'stary-oskol', 'syktyvkar',
                 'tver', 'tobolsk', 'tula', 'cheboksary', 'engels']
    city_name_rus = {'moscow': 'Г. Москва', 'astrakhan': 'Г. Астрахань', 'arkhangelsk': 'Г. Архангельск',
                     'novgorod': 'Г. Новгород', 'vladimir': 'Г. Владимир', 'volzhskiy': 'Г. Волжский',
                     'vyborg': 'Г. Выборг',
                     'gatchina': 'Г. Гатчина', 'gubkin': 'Г. Губкин', 'zheleznogorsk': 'Г. Железногорск',
                     'i-ola': 'Г. И-ола',
                     'kazan': 'Г. Казань', 'kaluga': 'Г. Калуга',
                     'kingisepp': 'Г. Кингисепп', 'kirov': 'Г. Киров', 'kursk': 'Г. Курск', 'lipetsk': 'Г. Липецк',
                     'luga': 'Г. Луга',
                     'magnitogorsk': 'Г. Магнитогорск',
                     'naberezhnye': 'Г. Набережные', 'nizhnevartovsk': 'Г. Нижневартовск',
                     'nizhniy-novgorod': 'Г. Нижний Новгород',
                     'nizhniy-tagil': 'Г. Нижне-Тагил', 'orel': 'Г. Орел', 'pervouralsk': 'Г. Первоуральск',
                     'petrozavodsk': 'Г. Петрозаводск',
                     'pskov': 'Г. Псков', 'ryazan': 'Г. Рязань', 'stary-oskol': 'Г. Старый Оскол',
                     'syktyvkar': 'Г. Сыктывкар',
                     'tver': 'Г. Тверь', 'tobolsk': 'Г. Тобольск', 'tula': 'Г. Тула', 'cheboksary': 'Г. Чебоксары',
                     'engels': 'Г. Энгельс'}
    for code in list_code:
        for city in city_name:
            print(code, city)
            get_links(code, city)
    df_to_db = df_to_db[df_to_db.measure_unit != '-']
    df_to_db['measure_unit'] = df_to_db['measure_unit'].replace('руб./м³', 'руб./куб. м')
    df_to_db['period'] = period
    df_to_db['year'] = int(period.year)
    df_to_db['month_name'] = int(period.month)
    df_to_db['month_name'] = df_to_db.month_name.map(month_str)
    df_to_db['region'] = df_to_db.region.map(city_name_rus)

    df_to_db['week'] = int(period.isocalendar()[1])

    df_to_db['indicator_value'] = df_to_db['indicator_value'].astype(str)

    df_to_db['indicator_value'] = df_to_db['indicator_value'].apply(
        lambda x: re.sub(r'\s+', '', x.replace(',', '.').strip()) if ',' in x else re.sub(r'\s+', '', x))
    df_to_db['indicator_value'] = df_to_db['indicator_value'].astype(float)
    df_to_db['measure_unit'] = df_to_db['measure_unit'].str.replace('Цена заштуку', 'руб./шт.')
    df_to_db['measure_unit'] = df_to_db['measure_unit'].str.replace('Цена замешок', 'руб./шт.')
    df_to_db['measure_unit'] = df_to_db['measure_unit'].str.replace('Цена заупаковку', 'руб./ед.')
    df_to_db['measure_unit'] = df_to_db['measure_unit'].str.replace('Цена зам2лист', 'руб./кв. м')
    df_to_db['measure_unit'] = df_to_db['measure_unit'].str.replace('Цена зам2рулон', 'руб./кв. м')

    print(df_to_db)
    print(df_to_db['measure_unit'].unique())
    print(df_to_db.info())
