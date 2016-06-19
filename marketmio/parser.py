import requests
import sqlite3
from bs4 import BeautifulSoup
import re
import aiohttp
import asyncio
class SQL:
    def __init__(self, test_db):
        self.connection = sqlite3.connect(test_db, check_same_thread=False)
        self.cursor = self.connection.cursor()

    def create_table(self):
        self.table = 'CREATE TABLE data (MAIN_CATEGORY TEXT, PRODUCT_CATEGORY TEXT, PRODUCTS_PAGE TEXT, ' \
                     'VENDOR TEXT, MODEL TEXT, DESCRIPTION TEXT, PRICE TEXT, URL TEXT, PIC_URL TEXT)'
        self.cursor.execute(self.table)

    def insert(self, main_category, subcat, name, vendor, model, descr, price, url, pic_url):
        query = 'INSERT into data VALUES ("{}", "{}", "{}", "{}", ' \
                '"{}", "{}", "{}", "{}", "{}")'.format(main_category, subcat, name, vendor, model, descr, price, url, pic_url)
        self.cursor.execute(query)
        self.connection.commit()


sql = SQL('marketmio.db')
try:
    sql.create_table()
except:
    pass

main_url = 'https://marketmio.ru'

maincats = [
            ('Компьютерная техника', 'https://marketmio.ru/category/91009'),
            ('Электроника', 'https://marketmio.ru/category/198119'),
            ('Авто', 'https://marketmio.ru/category/90402'),
            ('Детские товары', 'https://marketmio.ru/category/90764'),
            ('Бытовая техника', 'https://marketmio.ru/category/198118')]


def get_suburl(soup_obj):
    cats = [('category/91082', 'Источники бесперебойного питания')]
    for link in soup_obj.find_all('a'):
        if link.get('class') == ['select-category__link', 'link']:
            res, = re.findall('<a class="select-category__link link" href="/(.*?)" title="(.*?)">.*?</a>', str(link))
            cats.append(res)
    return cats


def get_content(link):
    content = []
    price = re.findall('\s(.*?)\sруб', str(link))
    url = re.findall('<a class="product__image-container" href="(.*?)" title=".*?">', str(link))
    name = re.findall('<a class="product__image-container" href=".*?" title="(.*?)">', str(link))
    pic = re.findall('<img class="product__image" src="(.*?)">', str(link))
    about = re.findall('Тип: .*', str(link))
    return zip(url, name, price, pic, about)


def run_parser():
    for name, url in maincats:
        print(name, url)
        page = requests.get(url).content.decode('utf8')
        soup = BeautifulSoup(page, 'html.parser')
        for suburl, subname in get_suburl(soup):
            suburl = main_url + '/' + suburl
            print(subname, suburl)
            for num in range(1, 51):
                u = suburl + '?onStock=1&page=' + str(num)
                print(u)
                prodpage = requests.get(u).content.decode('utf8')
                produrl = re.findall('<a class="product__image-container" href="(.*?)"', prodpage)
                fullname = re.findall('<a class="product__name-link" href=".*?" title=".*?">(.*?)</a>', prodpage)
                price = re.findall('.*от\s(.*?)\sруб.', prodpage)
                description = re.findall('Тип: (.*)\s', prodpage)
                pic_url = re.findall('<img src="(.*?)" class="product__image">', prodpage)
                assert len(produrl) == 12
                assert len(fullname) == 12
                assert len(price) == 12
                assert len(description) == 12
                assert len(pic_url) == 12
                data = zip(produrl, fullname, price, description, pic_url)
                for d in data:
                    print(d[1])
                    # MAIN_CATEGORY, PRODUCT_CATEGORY, PRODUCTS_PAGE, VENDOR, MODEL, DESCRIPTION, PRICE, URL, PIC_URL
                    try:
                        sql.insert(name, subname, u, d[1].split()[0], d[1].split()[1], d[3], d[2], main_url + d[0], d[4])
                    except:
                        sql.insert(name, subname, u, d[1], d[3], d[2], main_url + d[0], d[4])
        break


run_parser()


@asyncio.coroutine
def get(*args, **kwargs):
    response = yield from aiohttp.request('GET', *args, **kwargs)
    return (yield from response.text())


@asyncio.coroutine
def downlaod_html(urls):
    sem = asyncio.Semaphore(1000)
    with (yield from sem):
        page = yield from get(urls[0])
        html = parsing_lessons(page, urls[3])


def collect_urls(from_date, to_date, upd_type):
    urls = []
    for chat_id, email in mysql.search_all('users'):
        url = 'http://ruz.hse.ru/ruzservice.svc/personlessons?fromdate=' + from_date + '&todate=' + to_date + '&' \
              'receivertype=0&email=' + email
        url2 = 'http://hse.ru/api/timetable/lessons?fromdate=' + from_date + '&todate=' + to_date+ '&receiverType=4&email=' + email
        urls.append((url2, email, chat_id, upd_type))
    mysql.close_conn()
    print('Собрал все ссылки')
    return urls


def run_schedule_upd():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    print('Запускаю обновление')
    f = asyncio.wait([downlaod_html(d) for d in collect_urls(addition_days(1), addition_days(7), 'week')])
    loop = asyncio.get_event_loop()
    loop.run_until_complete(f)
