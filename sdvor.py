import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup
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


def get_links(articul, product, city, city_code):
    df = pd.DataFrame(columns=['indicator', 'subindicator_1', 'region', 'data_source', 'measure_unit', 'periodity','indicator_value'])
    url = f"https://www.sdvor.com/{city_code}/search/{product}"  # ссылка на товар
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
    site = requests.get(url, verify=False, headers=headers)  # получаем ответ от сайта
    # print(url, site)
    if site.status_code == 200:
        soup = BeautifulSoup(site.content, "html.parser")  # разбираем код страницы на элементы
        # сопоставляет код товара и артикул
        # print(city)
        if str(articul) == soup.find('div', {'class': 'product-code'}).text.split(':')[-1].strip():
            try:
                price = re.sub(r'\s+', ' ', soup.find('span', {'class': 'main'}).text).replace('₽', '').replace(' ','') # цена на товар
            except:
                price = re.sub(r'\s+', ' ', soup.find('div', {'class': 'price'}).text).replace('₽', '').replace(' ','')
            measure_unit = 'руб./' + soup.find('div', {'class': 'product-buy-section'}).text.split('₽')[-1].split()[
                0]  # единица измерения товара
            # print(city, product, price, measure_unit)

            df_row = pd.DataFrame(
                {'indicator': ['Цена на ' + product], 'subindicator_1': [str(articul)],'region': [city], 'data_source': 'Сайт ООО Строительный двор',
                 'measure_unit': [measure_unit], 'periodity': 'Еженедельно',
                 'indicator_value': price})  # создаем датафрейм из одной строки
            df = pd.concat([df, df_row])  # соединяем с общим датафремом
            df['indicator_value'] = df['indicator_value'].astype(float)
            global df_to_db
            df_to_db = pd.concat([df_to_db, df], ignore_index=True)
    else:
        pass
        print('Странички не открылась!!!', site.status_code)


if __name__ == '__main__':
    df_to_db = pd.DataFrame(columns=['indicator', 'subindicator_1', 'region', 'data_source', 'measure_unit', 'periodity','indicator_value'])
    products = {13905: 'Щебень гранитный в МКР (фр. 5-20 мм) 1000 кг',
                13908: 'Щебень гранитный 20/40, мкр',
                6432: 'Песок строительный крупнозернистый намывной, 1 т МКР',
                452723: 'Цемент ЦЕМ I 42,5Н (ПЦ-500 Д0), Евроцемент, 1 т',
                8274: 'Раствор- М-100',
                5780: 'Плита перекрытия ПК 60-12-8',
                60640: 'Битум, 25 кг',
                500677: 'Холодный асфальт, 25 кг',
                11251: 'Арматура 16 мм 2,9 м, класс А3',
                546434: 'Бетон В25 (M350)',
                130778: 'Кирпич одинарный полнотелый рядовой М-150 MSTERA',
                480450: 'Мат прошивной МП-100 (2000х1200х60)',
                28074: 'Лист оцинкованный 0,7мм 1,25х2,0м',
                217632: 'Доска обрезная 50х100х6000 мм (сорт 1-3) хвоя, естественной влажности',
                437595: 'Труба электросварная 57х3,5 мм, 3 м',
                83074: 'Кабель алюминиевый АВВГ 3х4 черный ГОСТ (бухта 75 м, отрез)',
                178722: 'Труба ПЭ 100 SDR17-0110х6,6 (12 метров)',
                452249: 'Блок газобетонный 600х300х200 мм В3,5 D500 Bonolit'
                }  # перечень артиулов и название материалов
    cities = {'Г. Екатеринбург': 'ekb', 'Г. Москва': 'moscow', 'Г. Павловский Посад': 'pavposad',
              'Г. Берёзовский': 'berezovsky', 'Г. Заводоуковск': 'zavodoukovsk', 'Г. Ишим': 'ishim',
              'Г. Лангепас': 'langepas', 'Г. Мегион': 'megion', 'Г. Нефтеюганск': 'nefteyugansk',
              'Г. Нижневартовск': 'nvartovsk', 'Г. Нижний Тагил': 'ntagil', 'Г. Пермь': 'perm', 'Г. Самара': 'samara',
              'Г. Сургут': 'surgut', 'Г. Тобольск': 'tobolsk', 'Г. Тюмень': 'tmn', 'Г. Челябинск': 'chelyabinsk',
              'Г. Ялуторовск': 'yalutorovsk'}  # перечень городов

    for articul, product in products.items(): # перебор всех материалов и городов
        print(articul, product)
        for city, city_code in cities.items():
            get_links(articul, product, city, city_code)
        break

    df_to_db['measure_unit'] = df_to_db['measure_unit'].replace('руб./м³', 'руб./куб. м')
    df_to_db['period'] = period
    df_to_db['year'] = int(period.year)
    df_to_db['month_name'] = int(period.month)
    df_to_db['month_name'] = df_to_db.month_name.map(month_str)
    df_to_db['week'] = int(period.isocalendar()[1])

    print(df_to_db)
    print(df_to_db.info())

