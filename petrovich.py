import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import time
import re
import urllib3
urllib3.disable_warnings()

pd.options.mode.chained_assignment = None
pd.set_option('display.max_rows', 550)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('max_colwidth', 70)

period = pd.to_datetime(datetime.now().date())  # дата
month_str = {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август',
             9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'}

df_to_db = pd.DataFrame(columns=['indicator', 'region', 'data_source', 'measure_unit', 'periodity', 'indicator_value'])

def get_links(code, city):
    url = f"https://{city}.petrovich.ru/catalog/9576106/{code}/"  # ссылка на товар
    print(url)

    cookies = {
        'SIK': 'egAAAESWtxUtocMTWngHAA',
        'SIV': '1',
        '_ym_uid': '1688571914487821770',
        'FPID': 'FPID2.2.YW2oGB4VGll8rFNS4ZTMQjDgxYnSbQhxxhkhUtwqHvA%3D.1688571914',
        'count_buy': '0',
        'js_SIK': 'egAAAESWtxUtocMTWngHAA',
        'ser_ym_uid': '1688571914487821770',
        'js_count_buy': '0',
        'js_FPID': 'FPID2.2.YW2oGB4VGll8rFNS4ZTMQjDgxYnSbQhxxhkhUtwqHvA%3D.1688571914',
        'ser_adrcid': 'AFX-C5fyBExKMopABxECWfA',
        'ssaid': 'eee0bde0-1b4a-11ee-bca7-69ac7a185cd9',
        '_userGUID': '0:lp1a8913:rws3mrwQRr2UFTbu89rEUEVxGnlf9gL8',
        'aplaut_distinct_id': 'TWcZey7WGny6',
        '_ymab_param': 'KMozXEcMcjUKR_B4eqHs2TsfI4HrrIjtJTtjxzn1Wh-27Ek-ZFt3E8sV21NdRiI6xlH3F1bc8zsy4Ccz6-lmzj6dgHU',
        'tmr_lvid': '7919ca6330b6ade7ae84629f0c58a8f5',
        'tmr_lvidTS': '1688571914514',
        'adrcid': 'AFX-C5fyBExKMopABxECWfA',
        'popmechanic_sbjs_migrations': 'popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1',
        '_gpVisits': '{"isFirstVisitDomain":true,"idContainer":"100025B4"}',
        'blueID': '4dd089b6-08d1-4db6-b0bc-876c76304017',
        'dd_user.isReturning': 'true',
        'ipp_uid_tst': '1705586090268/nzQCXpK1_LkvcjXWfwyyqQ',
        'ipp_sign': '18744c9d2c25908de750a007e6afe0eb_400371527_dfb45737556d1f4d591709ad1fca344a',
        'ipp_key': 'v1705586090294/v33947245bb5ad87a72e273/9N2BhTgH+HESbwc2SHKIyQ==',
        'ipp_uid': '1705586090294/CW5HdO3xpeOXq7ZS/hHlnvjCXNaJk1T4YDr2ZZQ==',
        'SNK': '151',
        'u__typeDevice': 'desktop',
        'u__geoCityGuid': 'b835705e-037e-11e4-9b63-00259038e9f2',
        'u__geoUserChoose': '1',
        'rerf': 'AAAAAGWpLatSwyquDBzNAg==',
        'C_72d4qhYFBsIJflXmm4HXIH0HWrE': 'AAAAAAAACEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA8D8AACCa6xfqQUBXzeKfgvGwrgqGA9W_kFU',
        '_ym_d': '1705586092',
        '_ym_isad': '2',
        '_gid': 'GA1.2.1353943415.1705586093',
        'FPLC': 'svbQllFM668drR%2FyCd5hiPxVIKi%2FqLF7kX8kdto%2BHm6wi1xwdl4IH8rQLcPchIxdtlbz46amylNVY0AgLjx6A%2BP6ZBv%2FdlMAu2mYMOIkQp%2FH1x98HdBQ8J4sEWbEzA%3D%3D',
        '_ym_visorc': 'b',
        'digiGroupNumber': 'group_2',
        'dSesn': '52ee4fdf-7aa7-f431-1505-e23ee7dfc116',
        '_dvs': '0:lrj9x9ga:H2UVymC2CRHhrnSbvLQgHdo~NV6_Kpg8',
        'dd_custom.lt15': '2024-01-18T13:55:15.358Z',
        'dd_custom.ts16': '{%22ttl%22:2592000%2C%22granularity%22:86400%2C%22data%22:{%221705536000%22:2}}',
        'dd__persistedKeys': '[%22custom.lastViewedProductImages%22%2C%22custom.lt15%22%2C%22custom.ts16%22%2C%22user.isReturning%22%2C%22custom.ts12%22%2C%22custom.lt11%22%2C%22custom.productsViewed%22]',
        'dd_custom.productsViewed': '9',
        'dd_custom.lastViewedProductImages': '[%22%22%2C%22%22%2C%2243558%22]',
        'dd_custom.ts12': '{%22ttl%22:2592000%2C%22granularity%22:86400%2C%22data%22:{%221705536000%22:9}}',
        'dd_custom.lt11': '2024-01-18T14:11:28.884Z',
        '__tld__': 'null',
        'dd__lastEventTimestamp': '1705587502052',
        '_ga_XW7S332S1N': 'GS1.1.1705586093.4.1.1705587504.0.0.0',
        '_ga': 'GA1.2.275570984.1700144651',
        '_gp100025B4': '{"utm":"-75345c70","hits":5,"vc":1,"ac":1,"a6":1}',
        'tmr_detect': '0%7C1705587515901',
        '_gat_popmechanicManualTracker': '1',
        'mindboxDeviceUUID': 'a05026df-95c1-4969-b68c-965699b36716',
        'directCrm-session': '%7B%22deviceGuid%22%3A%22a05026df-95c1-4969-b68c-965699b36716%22%7D',
    }
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        # 'Cookie': 'SIK=egAAAESWtxUtocMTWngHAA; SIV=1; _ym_uid=1688571914487821770; FPID=FPID2.2.YW2oGB4VGll8rFNS4ZTMQjDgxYnSbQhxxhkhUtwqHvA%3D.1688571914; count_buy=0; js_SIK=egAAAESWtxUtocMTWngHAA; ser_ym_uid=1688571914487821770; js_count_buy=0; js_FPID=FPID2.2.YW2oGB4VGll8rFNS4ZTMQjDgxYnSbQhxxhkhUtwqHvA%3D.1688571914; ser_adrcid=AFX-C5fyBExKMopABxECWfA; ssaid=eee0bde0-1b4a-11ee-bca7-69ac7a185cd9; _userGUID=0:lp1a8913:rws3mrwQRr2UFTbu89rEUEVxGnlf9gL8; aplaut_distinct_id=TWcZey7WGny6; _ymab_param=KMozXEcMcjUKR_B4eqHs2TsfI4HrrIjtJTtjxzn1Wh-27Ek-ZFt3E8sV21NdRiI6xlH3F1bc8zsy4Ccz6-lmzj6dgHU; tmr_lvid=7919ca6330b6ade7ae84629f0c58a8f5; tmr_lvidTS=1688571914514; adrcid=AFX-C5fyBExKMopABxECWfA; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; _gpVisits={"isFirstVisitDomain":true,"idContainer":"100025B4"}; blueID=4dd089b6-08d1-4db6-b0bc-876c76304017; dd_user.isReturning=true; ipp_uid_tst=1705586090268/nzQCXpK1_LkvcjXWfwyyqQ; ipp_sign=18744c9d2c25908de750a007e6afe0eb_400371527_dfb45737556d1f4d591709ad1fca344a; ipp_key=v1705586090294/v33947245bb5ad87a72e273/9N2BhTgH+HESbwc2SHKIyQ==; ipp_uid=1705586090294/CW5HdO3xpeOXq7ZS/hHlnvjCXNaJk1T4YDr2ZZQ==; SNK=151; u__typeDevice=desktop; u__geoCityGuid=b835705e-037e-11e4-9b63-00259038e9f2; u__geoUserChoose=1; rerf=AAAAAGWpLatSwyquDBzNAg==; C_72d4qhYFBsIJflXmm4HXIH0HWrE=AAAAAAAACEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA8D8AACCa6xfqQUBXzeKfgvGwrgqGA9W_kFU; _ym_d=1705586092; _ym_isad=2; _gid=GA1.2.1353943415.1705586093; FPLC=svbQllFM668drR%2FyCd5hiPxVIKi%2FqLF7kX8kdto%2BHm6wi1xwdl4IH8rQLcPchIxdtlbz46amylNVY0AgLjx6A%2BP6ZBv%2FdlMAu2mYMOIkQp%2FH1x98HdBQ8J4sEWbEzA%3D%3D; _ym_visorc=b; digiGroupNumber=group_2; dSesn=52ee4fdf-7aa7-f431-1505-e23ee7dfc116; _dvs=0:lrj9x9ga:H2UVymC2CRHhrnSbvLQgHdo~NV6_Kpg8; dd_custom.lt15=2024-01-18T13:55:15.358Z; dd_custom.ts16={%22ttl%22:2592000%2C%22granularity%22:86400%2C%22data%22:{%221705536000%22:2}}; dd__persistedKeys=[%22custom.lastViewedProductImages%22%2C%22custom.lt15%22%2C%22custom.ts16%22%2C%22user.isReturning%22%2C%22custom.ts12%22%2C%22custom.lt11%22%2C%22custom.productsViewed%22]; dd_custom.productsViewed=9; dd_custom.lastViewedProductImages=[%22%22%2C%22%22%2C%2243558%22]; dd_custom.ts12={%22ttl%22:2592000%2C%22granularity%22:86400%2C%22data%22:{%221705536000%22:9}}; dd_custom.lt11=2024-01-18T14:11:28.884Z; __tld__=null; dd__lastEventTimestamp=1705587502052; _ga_XW7S332S1N=GS1.1.1705586093.4.1.1705587504.0.0.0; _ga=GA1.2.275570984.1700144651; _gp100025B4={"utm":"-75345c70","hits":5,"vc":1,"ac":1,"a6":1}; tmr_detect=0%7C1705587515901; _gat_popmechanicManualTracker=1; mindboxDeviceUUID=a05026df-95c1-4969-b68c-965699b36716; directCrm-session=%7B%22deviceGuid%22%3A%22a05026df-95c1-4969-b68c-965699b36716%22%7D',
        'Referer': 'https://www.google.com/',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'cross-site',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-full-version': '"120.0.6099.217"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }
    params = {
        'version': '1.0.453',
        'transport': 'XmlHttpRequest',
        'operation': 'CheckSegment',
        'endpointId': 'petrovich.ru',
        'originDomain': 'moscow.petrovich.ru',
    }
    data = {
        'originDomain': 'moscow.petrovich.ru',
        'deviceUUID': 'a05026df-95c1-4969-b68c-965699b36716',
        'operation': 'CheckSegment',
        'ianaTimeZone': 'Europe/Moscow',
        'endpointId': 'petrovich.ru',
        'data': '{"segmentations":[{"ids":{"externalId":"740543e8-fe70-4c54-bf7b-a662e8733e47"}},{"ids":{"externalId":"d0c57932-dc4e-40b8-bfe4-4046b219f3da"}},{"ids":{"externalId":"34713979-0cb0-4af2-8756-be8972a30b36"}},{"ids":{"externalId":"33ade289-22cd-43c8-a59d-10f9604eed72"}},{"ids":{"externalId":"3ffbc7e0-b6d8-443c-945c-d1d797ee54b3"}},{"ids":{"externalId":"8f92be67-e07b-47b6-8468-5b626b52e53a"}},{"ids":{"externalId":"03465ddb-f591-4611-9061-14ea00a7cd84"}}]}',
    }

    try:
        time.sleep(20)
        site = requests.get(url, verify=False, params=params, cookies=cookies, headers=headers,
                            data=data,timeout=(10, 0.01))  # получаем ответ от сайта
        soup = BeautifulSoup(site.content, "html.parser")  # разбираем код страницы на элементы
        if site.status_code == 200:
            try:
                price = soup.find('p', {'data-test': 'product-retail-price'}).text.replace('₽', '')  # цена на товар
                measure_unit = soup.find('span', {'class': 'units-tabs'}).text
            except:
                try:
                    price = soup.find('p', {'data-test': 'product-gold-price'}).text.replace('₽', '')  # цена на товар
                    measure_unit = soup.find('span', {'class': 'units-tabs'}).text
                except:
                    price = soup.find('div', {'data-test': 'product-status'}).text
                    measure_unit = '-'

            try:
                name_product = soup.find('h1', {'data-test': 'product-title'}).text  # имя товара
                region = soup.find('span', {'data-test': 'region-link'}).text  # регион
            except:
                region = soup.find('div', {'class': 'pt-y-center'}).text.split()[0].strip()  # регион
                name_product = soup.find('h1', {'data-test': 'product-title'}).text  # имя товара

            print(f'Имя товара: {name_product} \nЦена: {price} \nРегион:{region}\nЕдиница измерения:{measure_unit}')
            df_row = pd.DataFrame(
                {'indicator': ['Цена на ' + name_product], 'region': ['Г. ' + region],
                 'data_source': 'Сайт Строительный Торговый Дом Петрович', 'measure_unit': [measure_unit],
                 'periodity': 'Еженедельно', 'indicator_value': price})  # создаем датафрейм из одной строки
            global df_to_db
            df_to_db = pd.concat([df_to_db, df_row], ignore_index=True)

        else:
            print(f'Status site, {site.status_code}')
            pass
    except:
        print('Big problem!!!')



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
    city_name = ['moscow','arkhangelsk', 'astrakhan', 'novgorod', 'vladimir', 'volzhskiy', 'vyborg',
                 'gatchina', 'gubkin', 'zheleznogorsk', 'i-ola', 'kazan', 'kaluga', 'kingisepp', 'kirov', 'kursk',
                 'lipetsk', 'luga', 'magnitogorsk', 'naberezhnye', 'nizhnevartovsk', 'nizhniy-novgorod',
                 'nizhniy-tagil', 'orel', 'pervouralsk', 'petrozavodsk', 'pskov', 'ryazan', 'stary-oskol', 'syktyvkar',
                 'tver', 'tobolsk', 'tula', 'cheboksary', 'engels']
    for code in list_code:
        for city in city_name:
            print(code, city)
            get_links(code, city)

    df_to_db['measure_unit'] = df_to_db['measure_unit'].replace('руб./м³', 'руб./куб. м')
    df_to_db['period'] = period
    df_to_db['year'] = int(period.year)
    df_to_db['month_name'] = int(period.month)
    df_to_db['month_name'] = df_to_db.month_name.map(month_str)
    df_to_db['week'] = int(period.isocalendar()[1])

    df_to_db = df_to_db[df_to_db.indicator_value != 'Товар недоступен в этом городе']
    df_to_db = df_to_db[df_to_db.indicator_value != 'Отсутствует в городе']
    print(df_to_db)
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
