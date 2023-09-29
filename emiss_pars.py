"""
Инвестиции в основной капитал по полному кругу хозяйствующих субъектов

"""
from urllib import request
from datetime import datetime
import pandas as pd
import numpy as np

pd.options.mode.chained_assignment = None

pd.set_option('display.max_rows', 550)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('max_colwidth', 70)

tday = str(datetime.now().date())
id = 59047
file_name = f"emiis_{str(id)}_{tday}.xls"


def get_filters():
    """
    Функция фозвращает набор фильтров, включая заданные периоды

    :param period_list: необходимые периоды для выгрузки отчета
    :return: готовый набор фильтров
    """
    # Список уникальных фильров для каждого отчета (настраивается для каждого отчета)
    payload = ['lineObjectIds=0',
               'lineObjectIds=30611',
               'lineObjectIds=33560',
               'lineObjectIds=57831',
               'lineObjectIds=58121',
               'columnObjectIds=3',
               'selectedFilterIds=0_59047',

               'selectedFilterIds=3_2017',
               'selectedFilterIds=3_2018',
               'selectedFilterIds=3_2019',
               'selectedFilterIds=3_2020',
               'selectedFilterIds=3_2021',

               'selectedFilterIds=30611_950352',
               'selectedFilterIds=33560_1558883',
               'selectedFilterIds=57831_1688487',
               'selectedFilterIds=57831_1688488',
               'selectedFilterIds=57831_1688489',
               'selectedFilterIds=57831_1688490',
               'selectedFilterIds=57831_1688491',
               'selectedFilterIds=57831_1688492',
               'selectedFilterIds=57831_1688493',
               'selectedFilterIds=57831_1688494',
               'selectedFilterIds=57831_1688495',
               'selectedFilterIds=57831_1688496',
               'selectedFilterIds=57831_1688497',
               'selectedFilterIds=57831_1688498',
               'selectedFilterIds=57831_1688499',
               'selectedFilterIds=57831_1688500',
               'selectedFilterIds=57831_1688501',
               'selectedFilterIds=57831_1688502',
               'selectedFilterIds=57831_1688503',
               'selectedFilterIds=57831_1688504',
               'selectedFilterIds=57831_1688505',
               'selectedFilterIds=57831_1688506',
               'selectedFilterIds=57831_1688507',
               'selectedFilterIds=57831_1688508',
               'selectedFilterIds=57831_1688509',
               'selectedFilterIds=57831_1688510',
               'selectedFilterIds=57831_1688511',
               'selectedFilterIds=57831_1688513',
               'selectedFilterIds=57831_1688514',
               'selectedFilterIds=57831_1688515',
               'selectedFilterIds=57831_1688516',
               'selectedFilterIds=57831_1688517',
               'selectedFilterIds=57831_1688518',
               'selectedFilterIds=57831_1688519',
               'selectedFilterIds=57831_1688522',
               'selectedFilterIds=57831_1688523',
               'selectedFilterIds=57831_1688524',
               'selectedFilterIds=57831_1688525',
               'selectedFilterIds=57831_1688526',
               'selectedFilterIds=57831_1688527',
               'selectedFilterIds=57831_1688528',
               'selectedFilterIds=57831_1688529',
               'selectedFilterIds=57831_1688530',
               'selectedFilterIds=57831_1688531',
               'selectedFilterIds=57831_1688532',
               'selectedFilterIds=57831_1688533',
               'selectedFilterIds=57831_1688534',
               'selectedFilterIds=57831_1688535',
               'selectedFilterIds=57831_1688536',
               'selectedFilterIds=57831_1688537',
               'selectedFilterIds=57831_1688538',
               'selectedFilterIds=57831_1688539',
               'selectedFilterIds=57831_1688540',
               'selectedFilterIds=57831_1688541',
               'selectedFilterIds=57831_1688542',
               'selectedFilterIds=57831_1688543',
               'selectedFilterIds=57831_1688545',
               'selectedFilterIds=57831_1688546',
               'selectedFilterIds=57831_1688547',
               'selectedFilterIds=57831_1688548',
               'selectedFilterIds=57831_1688549',
               'selectedFilterIds=57831_1688550',
               'selectedFilterIds=57831_1688551',
               'selectedFilterIds=57831_1688552',
               'selectedFilterIds=57831_1688553',
               'selectedFilterIds=57831_1688554',
               'selectedFilterIds=57831_1688555',
               'selectedFilterIds=57831_1688556',
               'selectedFilterIds=57831_1688557',
               'selectedFilterIds=57831_1688559',
               'selectedFilterIds=57831_1688560',
               'selectedFilterIds=57831_1688561',
               'selectedFilterIds=57831_1688562',
               'selectedFilterIds=57831_1688563',
               'selectedFilterIds=57831_1688564',
               'selectedFilterIds=57831_1688565',
               'selectedFilterIds=57831_1688566',
               'selectedFilterIds=57831_1688568',
               'selectedFilterIds=57831_1688571',
               'selectedFilterIds=57831_1688573',
               'selectedFilterIds=57831_1688574',
               'selectedFilterIds=57831_1688575',
               'selectedFilterIds=57831_1688576',
               'selectedFilterIds=57831_1688577',
               'selectedFilterIds=57831_1688578',
               'selectedFilterIds=57831_1688579',
               'selectedFilterIds=57831_1688581',
               'selectedFilterIds=57831_1688582',
               'selectedFilterIds=57831_1688583',
               'selectedFilterIds=57831_1688584',
               'selectedFilterIds=57831_1688585',
               'selectedFilterIds=57831_1688586',
               'selectedFilterIds=57831_1688587',
               'selectedFilterIds=57831_1692937',
               'selectedFilterIds=57831_1692938',
               'selectedFilterIds=57831_1692939',
               'selectedFilterIds=57831_1692940',
               'selectedFilterIds=57831_1695534',
               'selectedFilterIds=57831_1795276',
               'selectedFilterIds=57831_1795277',
               'selectedFilterIds=58121_1704560',
               'selectedFilterIds=58121_1704562',
               'selectedFilterIds=58121_1704563',
               'selectedFilterIds=58121_1704570',
               'selectedFilterIds=58121_1704586',
               'selectedFilterIds=58121_1704611',
               ]
    payload_difference_filtered_period = sorted(list(set(payload)))

    return '&'.join(payload_difference_filtered_period)


def get_excel():
    """
    Скачивание файла отчета с указанными фильтрами

    :param id: id отчета
    :param filters: фильтры (настройки) для файла отчета
    """
    url_excel = f'https://fedstat.ru/indicator/data.do?format=excel&id={str(id)}&{get_filters()}'
    request.urlretrieve(url_excel, file_name)
    return file_name


def cleaning_df():
    """

    Получение и обработка датафрейма. Приведение к нормальной форме.

    """
    df_unpivot = pd.read_excel(get_excel())
    df_unpivot.drop([df_unpivot.columns[0], df_unpivot.columns[1], df_unpivot.columns[2]], axis='columns', inplace=True)
    df_unpivot.dropna(thresh=3, inplace=True)
    df_unpivot.reset_index(inplace=True)
    del df_unpivot['index']
    df_unpivot.rename(columns=df_unpivot.iloc[0], inplace=True)
    df_unpivot.drop(index=[0], inplace=True)
    col_one_list = df_unpivot.columns.to_list()
    col_one_list[0], col_one_list[1] = 'region', 'subindicator_1'
    df_unpivot.columns = col_one_list
    df_unpivot = pd.melt(df_unpivot, id_vars=col_one_list[:2], value_vars=col_one_list[2:],
                         value_name='indicator_value')
    df_unpivot.rename(columns={'variable': 'year'}, inplace=True)
    df_unpivot['year'] = df_unpivot['year'].astype(np.int64)
    df_unpivot['indicator_value'] = df_unpivot['indicator_value'].astype(float)
    df_unpivot['period'] = df_unpivot['year'].astype(str) + '.' + '01' + '.' + '01'
    df_unpivot['period'] = pd.to_datetime(df_unpivot['period'], format='%Y.%m.%d')
    df_unpivot['subindicator_1'] = df_unpivot['subindicator_1'].str.capitalize()
    df_unpivot['region'] = df_unpivot['region'].str.strip()
    df_unpivot['subindicator_1'] = df_unpivot['subindicator_1'].str.strip()
    print(df_unpivot)
    print(df_unpivot.info())


if __name__ == '__main__':
    cleaning_df()
