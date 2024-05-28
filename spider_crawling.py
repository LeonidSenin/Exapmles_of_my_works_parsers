import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import colorama
import pandas as pd

pd.set_option('display.max_rows', 550)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('max_colwidth', 70)

# инициализация модуля colorama
colorama.init()
GREEN = colorama.Fore.GREEN
GRAY = colorama.Fore.LIGHTBLACK_EX
RESET = colorama.Fore.RESET
YELLOW = colorama.Fore.YELLOW

# инициализация наборов для ссылок (обеспечивается уникальность ссылок)
'''internal_urls — URL-адреса, которые ведут на другие страницы того же веб-сайта.
external_urls — URL-адреса, которые ведут на другие веб-сайты.'''
internal_urls = set()
external_urls = set()
total_urls_visited = 0

def is_valid(url):
    """
    Проверяет, является ли 'url' действительным URL
    """
    parsed = urlparse(url)
    # print(parsed)
    # print(bool(parsed.netloc) and bool(parsed.scheme))
    return bool(parsed.netloc) and bool(parsed.scheme)

def get_all_website_links(url):
    """
    Возвращает все URL-адреса, найденные на `url`, в котором он принадлежит тому же веб-сайту.
    Параметры: url (str): URL-адрес.
    """
    # все URL-адреса `url`
    urls = set()
    external_urls_test = set()
    # доменное имя URL без протокола
    domain_name = urlparse(url).netloc
    soup = BeautifulSoup(requests.get(url,verify=False).content, "html.parser")

    # site = requests.get("https://rosstat.gov.ru/investment_nonfinancial", verify=False)
    # soup = BeautifulSoup(site.content, "html")

    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        if href == "" or href is None:
            # href пустой тег
            continue
        href = urljoin(url, href)
        parsed_href = urlparse(href)
        # удалить GET-параметры URL, фрагменты URL и т. д.
        href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
        if not is_valid(href):
            # недействительный URL
            continue
        if href in internal_urls:
            # уже в наборе
            continue
        if domain_name not in href:
            # внешняя ссылка
            if href not in external_urls:
                print(f"{GRAY}[!] Внешняя ссылка: {href}{RESET}")
                external_urls.add(f"{href}")
                external_urls_test.add(f"{href}")
            continue
        print(f"{GREEN}[*] Внутренняя ссылка: {href}{RESET}")
        urls.add(f"{href}")
        internal_urls.add(href)
    print(external_urls_test)
    return urls,external_urls_test

def crawl(url):
    """
        Ведет подсчет всех источников (внешние, внутренние), а так же вызывает функцию сбора get_all_website_links .
        Параметры: url (str): URL-адрес.
    """
    global total_urls_visited
    total_urls_visited += 1
    try:
        internal_links,external_links = get_all_website_links(url)
        return external_links
    except:
        return ['np.nan']
def group_dataframe (list_of_first_itter,n_count):
    """
        Проходит все сслыки (внешние, внутренние) и собирает их (внешние) в датафрейм
        Параметры: list_of_first_itter (list): ссылки на источники, n_count (int): число задаваемых иттераций.
    """
    df_start = pd.DataFrame(list_of_first_itter, columns=['itteration_0'])
    count = 1
    # n_count = 2
    while count <= n_count:
        df_conc = pd.DataFrame()
        for i, row in df_start.iterrows():
            df = pd.DataFrame()
            df[f'itteration_{count}'] = list(crawl(row[f'itteration_{count - 1}']))
            df[f'itteration_{count - 1}'] = row[f'itteration_{count - 1}']
            df_conc = pd.concat([df_conc, df])
            # print(df_conc.info())
        df_start = pd.merge(df_start, df_conc, on=f'itteration_{count - 1}', how='inner')
        print(df_start)
        print(df_start.info())
        count += 1
    return df_start

if __name__ == "__main__":
    # Добавить цикл для прохода всех ссылок с первой иттерации
    list_of_first_itter = ['https://www.marronnier.ru/blog/15-analitika/52-avtomatizitsiya-klasterizatsiya-klyuchevyh-slov-na-python']
    # Количество иттераций
    n_count = 2

    df_start = group_dataframe(list_of_first_itter,n_count)

    print("[+] Итого внутренних ссылок:", len(internal_urls))
    print("[+] Итого внешних ссылок:", len(external_urls))
    print("[+] Итого URL:", len(external_urls) + len(internal_urls))
    print(df_start)
    # print(df_start.loc[(df_start['itteration_2'] == 'tel://+78007779075')])



