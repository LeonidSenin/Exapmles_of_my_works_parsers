"""
Внутренние затраты на научные исследования и разработки по субъектам Российской Федерации

"""
# Импорт необходимых библиотек
import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np
import re
import urllib3

urllib3.disable_warnings()

# Параметры для отображения датафреймов в логах
pd.options.mode.chained_assignment = None
pd.set_option('display.max_rows', 550)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('max_colwidth', 70)

month_str = {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август',
             9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'}


def get_links():
    url = f"https://rosstat.gov.ru/statistics/science"  # ссылка на товар
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
    site = requests.get(url, verify=False, headers=headers)  # получаем ответ от сайта
    links = []
    if site.status_code == 200:
        soup = BeautifulSoup(site.content, "html.parser")
        for i in soup.findAll('div', {'class': 'document-list__item document-list__item--row'}):
            for j in i.findAll('div', {'class': 'document-list__item-title'}):
                if 'Внутренние затраты на научные исследования и разработки' in re.sub(r'\s+', ' ', j.text):
                    link = 'https://rosstat.gov.ru' + i.find('a', {'class': 'btn btn-icon btn-white btn-br btn-sm'}).get('href')
                    links.append(link)
    else:
        print(f"Status code: {site.status_code}")
    return links

def clean_df():
    for i in get_links():
        df = pd.read_excel(i, sheet_name=2)
        df.dropna(thresh=3, inplace=True)
        df.reset_index(inplace=True)
        del df['index']
        df.rename(columns=df.iloc[0], inplace=True)
        df.drop(index=[0], inplace=True)
        col_one_list = df.columns.to_list()
        col_one_list[0] = 'region'
        df.columns = col_one_list
        df_unpivot = pd.melt(df, id_vars=col_one_list[:1], value_vars=col_one_list[1:],
                             value_name='indicator_value')
        df_unpivot.rename(columns={'variable': 'year'}, inplace=True)
        df_unpivot['year'] = df_unpivot['year'].apply(lambda x: int(str(x).split()[0].split('.')[0]))
        df_unpivot['indicator_value'] = df_unpivot['indicator_value'].apply(
            lambda x: np.nan if ')' in str(x) else x)
        df_unpivot['indicator_value'] = df_unpivot['indicator_value'].astype(float)
        df_unpivot['period'] = df_unpivot['year'].astype(str) + '.' + '01' + '.' + '01'
        df_unpivot['period'] = pd.to_datetime(df_unpivot['period'], format='%Y.%m.%d')
        df_unpivot['region'] = df_unpivot['region'].apply(
            lambda x: re.sub("\d+", "", x.split(')')[0]) if ')' in x else x)
        df_unpivot['region'] = df_unpivot['region'].str.replace('в том числе', '')
        df_unpivot['region'] = df_unpivot['region'].str.strip()

        print(df_unpivot)
    # print(df_unpivot['region'].unique())
    # print(df_unpivot.info())


if __name__ == '__main__':
    clean_df()
