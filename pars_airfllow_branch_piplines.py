"""
Индекс промышленного производства по странам. Данные ежемесячные/ежеквартальные (сайт ОЭСР)
"""
# Импорт необходимых библиотек
import pandas as pd
import requests
import json
import os
from datetime import timedelta, datetime
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.edgemodifier import Label
from airflow.decorators import dag, task
from airflow.models import Variable
from utils import DatabaseOperation, TransformOperation

OPERATOR = DatabaseOperation("l_senin")
TRANSFORM_OPERATOR = TransformOperation()

# Параметры для отображения датафреймов в логах
pd.set_option('display.max_rows', 550)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('max_colwidth', 55)

# Импорт переменных из глобальных переменных среды Airflow
credentials = Variable.get("l_senin", deserialize_json=True)
os.chdir(Variable.get("data_dir"))

# Блок иморта глобальных переменных
# Переменные для dag
default_args = {'owner': 'L.Senin',
                'start_date': datetime(2023, 1, 1),
                'retries': 3,
                'retry_delay': timedelta(seconds=250)}

TABLE_NAME = 'idps_039_industrial_production_index_m_q'
SCHEMA_NAME = 'raw_data'

month_dict = {'January': '01', 'February': '02', 'March': '03', 'April': '04', 'May': '05', 'June': '06',
              'July': '07', 'August': '08',
              'September': '09', 'October': '10', 'November': '11', 'December': '12'}
month_dict_1 = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12', 'Q1': '03', 'Q2': '06', 'Q3': '09', 'Q4': '12'}
month_dict_2 = {'1': 'январь', '2': 'февраль', '3': 'март', '4': 'апрель', '5': 'май', '6': 'июнь', '7': 'июль',
                '8': 'август', '9': 'сентябрь', '10': 'октябрь', '11': 'ноябрь', '12': 'декабрь'}


@task
def parse():
    response = requests.get(
        'https://stats.oecd.org/SDMX-JSON/data/MEI_ARCHIVE/AUS+AUT+BEL+CAN+CHL+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+NMEC+BRA+CHN+IND+IDN+RUS+ZAF.201.202207+202208+202209+202210+202211+202212+202301+202302+202303+202304+202305+202306+202307+202308+202309.Q+M/all?startTime=2022-Q2&endTime=2023-Q3&dimensionAtObservation=allDimensions',
        verify=False)
    data = json.loads(response.text)

    # Словари
    dict_country = {}
    dict_iip = {}
    dict_month_date = {}
    dict_m_q = {}
    dict_m_year = {}

    list_of_dicts = [dict_country, dict_iip, dict_month_date, dict_m_q, dict_m_year]
    count_of_dicts_in_list = 0

    for status in data["structure"]['dimensions']['observation']:
        count = 0
        for i in status['values']:
            list_of_dicts[count_of_dicts_in_list][count] = i['name']
            count += 1
        count_of_dicts_in_list += 1

    main_list = []
    for key, value in data["dataSets"][0]['observations'].items():
        main_list.append([dict_country.get(int(key.split(':')[0])),
                          dict_month_date.get(int(key.split(':')[2])), dict_m_q.get(int(key.split(':')[3])),
                          dict_m_year.get(int(key.split(':')[4])), value[0]])

    df = pd.DataFrame.from_records(main_list, columns=['region', 'report_date', 'm/q', 'period',
                                                       'indicator_value'])
    df['report_date_1'] = df['report_date'].apply(lambda x: x.split()[0])
    df['report_date_1'] = '01.' + df.report_date_1.map(month_dict) + '.' + df['report_date'].apply(
        lambda x: x.split()[-1])
    df['report_date'] = pd.to_datetime(df['report_date_1'], format='%d.%m.%Y')
    del df['report_date_1']

    df['period_1'] = df['period'].apply(lambda x: x.split('-')[0])
    df['period_1'] = '01.' + df.period_1.map(month_dict_1) + '.' + df['period'].apply(
        lambda x: x.split('-')[-1])
    df['period'] = pd.to_datetime(df['period_1'], format='%d.%m.%Y')
    del df['period_1']
    df['year'] = df['period'].apply(lambda x: int(x.year))

    df['month_name'] = df['period'].apply(lambda x: str(x.month))
    df['month_name'] = df['month_name'].map(month_dict_2)
    df['month_name'] = df['month_name'].str.capitalize()

    # print(df)
    # print(df.info())
    # print(df['period'].unique())

    df.to_pickle(TABLE_NAME + '.pkl')
    print('Transform - done')


@task
def transform(table_name):
    df = pd.read_pickle(TABLE_NAME + '.pkl')
    if table_name == 'idps_039_industrial_production_index_m':
        print('month')
        df = df[~df['m/q'].str.contains('Quarterly')]
        del df['m/q']
    elif table_name == 'idps_040_industrial_production_index_q':
        print('quarter')
        df = df[~df['m/q'].str.contains('Monthly')]
        del df['m/q']
        df['quarter'] = df['month_name'].apply(lambda x: 4 if x == 'Декабрь' else (
            3 if x == 'Сентябрь' else (2 if x == 'Июнь' else (1 if x == 'Март' else x))))

    df.to_pickle(table_name + '.pkl')
    print(df)
    print(df.info())
    print("Save: " + table_name + '.pkl')


@dag(
    TABLE_NAME,
    catchup=False,
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    description="Индекс промышленного производства по странам",
    schedule_interval='0 1 1 3,6,9,12 *',
    tags=['idps']
)
def etl():
    from utils.ac_operators import send_telegram_message

    # Начинаем формировать пайплайн. Задаем связку до разветвления
    transform_data_operator = parse()

    # Начинаем разветвление
    tables = ["idps_039_industrial_production_index_m",
              "idps_040_industrial_production_index_q"]  # Условия разветвления

    for table in tables:
        # Пробегаемся по элементам. Создаем таски для каждого элемента
        postfix = table[-2:]

        merge = PythonOperator(
            task_id='merge' + postfix,
            python_callable=OPERATOR.merge_global,
            op_kwargs={
                "file_name": table + '.pkl',
                "columns_merge": ['region', 'report_date', 'period', 'indicator_value'],
                "table_name": table,
                "schema_name": SCHEMA_NAME,
                "show_info": True,  # Вывод информации о данных, готовых к записи в БД
                # "write_to_db": False,
            }
        )

        write_number_rows = PostgresOperator(
            task_id='write_number_rows' + postfix,
            postgres_conn_id="PostgreSQL_48",
            sql="sql/insert_row_numbers.sql",
            params={"table_name": table, 'task_id': 'merge' + postfix},
            trigger_rule='all_success'
        )

        update_version = PythonOperator(
            task_id='Update_version' + postfix,
            python_callable=OPERATOR.update_version_global,
            op_kwargs={
                "table_name": table,
                "schema_name": SCHEMA_NAME,
                "partition_by_columns": ['region', 'report_date', 'period'],
            }
        )

        # "Цепляемся" к последнему звену пайплайна и создаем цепочки из ветвлений
        transform_data_operator >> Label(table) >> transform.override(task_id=f'transform{postfix}')(
            table) >> merge >> write_number_rows >> update_version >> send_telegram_message


etl()