# Импорт необходимых библиотек
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from seleniumwire import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import time
import re
from dateutil.relativedelta import relativedelta
from datetime import datetime
import os

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.decorators import dag, task
from utils import DatabaseOperation, TransformOperation

OPERATOR = DatabaseOperation("l_senin")
TRANSFORM_OPERATOR = TransformOperation()
os.chdir(Variable.get("data_dir"))

TABLE_NAME = 'pmp_008_sme_num_m'
SCHEMA_NAME = 'raw_data'
default_args = {'owner': 'L.Senin', 'start_date': datetime(2023, 1, 1)}

# Параметры для отображения датафреймов в логах
pd.options.mode.chained_assignment = None
pd.set_option('display.max_rows', 550)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('max_colwidth', 70)

options = webdriver.ChromeOptions()
options.add_argument('ignore-certificate-errors')
options.add_argument('--no-sandbox')
options.add_argument("user-agent=Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0) Gecko/20100101 Firefox/84.0")
options.add_argument("--headless")
options.add_argument('--disable-dev-shm-usage')
options.add_argument("--disable-gpu")

month_str = {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август',
             9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'}

@task
def parse():
    url = f"https://rmsp.nalog.ru/statistics.html"  # ссылка на источник
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.implicitly_wait(30)
    driver.get(url)
    element = driver.find_element(By.XPATH, "/html/body/div[1]/div[3]/div/div[1]/div[1]/div/ul[1]/li[3]/a")  # ищем элемент по id
    time.sleep(10)
    element.click()  # кликаем
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    period = pd.to_datetime(re.findall('\d+.\d+.\d\d\d\d',
                                    soup.find('div', {'class': 'form-field'}).text)[0],format='%d.%m.%Y') - relativedelta(months=1)
    for i in soup.findAll('div', {'class': 'table-container'}):
        df = pd.DataFrame(columns = ['region','Всего;Всего','Юридические лица;Микро','Юридические лица;Малое','Юридические лица;Среднее',
                                    'Индивидуальные предприниматели;Микро','Индивидуальные предприниматели;Малое',
                                    'Индивидуальные предприниматели;Среднее'])
        for j in i.find_all('tr'):
            row_data = j.find_all('td')
            row = [i.text for i in row_data]
            if len(row) > 4:
                length = len(df)
                df.loc[length] = row
        col_one_list = df.columns.to_list()
        # print(df)
        df = pd.melt(df, id_vars=col_one_list[:1], value_vars=col_one_list[1:], value_name='indicator_value')
        new = df['variable'].str.split(';', expand=True)
        df = pd.concat([df, new], axis=1)
        del df['variable']
        df.rename(columns={0: 'subindicator_1', 1: 'subindicator_2'}, inplace=True)
        df['region_id'] = df['region'].apply(lambda x: re.search(r'\d+', x).group(0)
        if re.search(r'\d+', x) is not None else np.nan)
        df['region'] = df['region'].apply(lambda x: re.sub(r'\d+','',x))
        df['indicator_value'] = df['indicator_value'].apply(lambda x: x.replace(' ',''))
        df['period'] = period

        df['month_num'] = df['period'].apply(lambda x: x.month)
        df['year'] = df['period'].apply(lambda x: x.year)
        df['month_name'] = df.month_num.map(month_str)
        df.drop(columns=['month_num'], inplace=True)
        df['indicator_value'] = df['indicator_value'].astype(float)
        df['region_id'] = df['region_id'].astype(float)
        df = TRANSFORM_OPERATOR.add_similar_region_column_ml(df, test_ml_classification=False)

        df.to_pickle(f'{TABLE_NAME}.pkl')

        print(df)
        print(df.info())

    driver.quit()

@dag(TABLE_NAME,
     catchup=False,
     default_args=default_args,
     description="Сведения о количестве ЮЛ, ИП, в том числе по данным МСП",
     schedule_interval='0 0 11 * *',
     tags=['pmp', 'Сведения о количестве ЮЛ, ИП, в том числе по данным МСП']
     )
def etl():
    from utils.ac_operators import send_telegram_message

    merge = PythonOperator(
        task_id='merge',
        python_callable=OPERATOR.merge_global,
        op_kwargs={
            "file_name": TABLE_NAME + '.pkl',
            "columns_merge": ['region','indicator_value','subindicator_1','subindicator_2','region_id','period','year','month_name'],
            "table_name": TABLE_NAME,
            "schema_name": SCHEMA_NAME,
            # "write_to_db":False
        }
    )

    update_version = PythonOperator(
        task_id='Update_version',
        python_callable=OPERATOR.update_version_global,
        op_kwargs={
            "table_name": TABLE_NAME,
            "schema_name": SCHEMA_NAME,
            "partition_by_columns": ['region','subindicator_1','subindicator_2','region_id','period'],
        }
    )

    write_number_rows = PostgresOperator(
        task_id="write_number_rows",
        postgres_conn_id="PostgreSQL_48",
        sql="sql/insert_row_numbers.sql",
        params={"table_name": TABLE_NAME, 'task_id': 'merge'},  # task_1 - сюда подставляем task_id оператора, в котором вызывается функция, возвращающая значение
        trigger_rule='all_success'
    )

    # Инициируем пайплайн
    parse() >>  merge >> update_version >> write_number_rows >> send_telegram_message

etl()