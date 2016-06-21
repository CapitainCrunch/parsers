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
                     'FULLNAME TEXT, DESCRIPTION TEXT, PRICE TEXT, URL TEXT, PIC_URL TEXT)'
        self.cursor.execute(self.table)

    def insert(self, main_category, subcat, name, fullname, descr, price, url, pic_url):
        query = 'INSERT into data VALUES ("{}", "{}", "{}", "{}", ' \
                '"{}", "{}", "{}", "{}")'.format(main_category, subcat, name, fullname, descr, price, url, pic_url)
        self.cursor.execute(query)
        self.connection.commit()


sql = SQL('marketmio_test.db')
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
    cats = []
    for link in soup_obj.find_all('a'):
        if link.get('class') == ['select-category__link', 'link']:
            res, = re.findall('<a class="select-category__link link" href="(/.*?)" title="(.*?)">.*?</a>', str(link))
            cats.append(res)
    return cats

regex_produrl = re.compile('<a class="product__image-container ?e?v?e?n?t?" href="(.*?)"')
regex_fullname = re.compile('<a class="product__name-link ?e?v?e?n?t?" href=".*?".*? title=".*?">(.*?)</a>')
regex_price = re.compile('>?\s?(.*?)\sруб.')
regex_description = re.compile('<a class="product__description.*?">\s*(.*?)\s.*?<ul class="bullet-list">')
regex_pic_url = re.compile('<img src="(.*?jpe?g)" class="product__image">', re.IGNORECASE)

def parse_goods(prodpage):
    produrl = regex_produrl.findall(prodpage)
    fullname = regex_fullname.findall(prodpage)
    price = regex_price.findall(prodpage)
    description = regex_description.findall(prodpage)
    pic_url = regex_pic_url.findall(prodpage)
    assert len(produrl) == 12, len(produrl)
    assert len(fullname) == 12, len(fullname)
    assert len(price) == 12, len(price)
    assert len(description) == 12, len(description)
    assert len(pic_url) == 12, len(pic_url)
    data = zip(produrl, fullname, price, description, pic_url)
    return data


@asyncio.coroutine
def get(*args, **kwargs):
    response = yield from aiohttp.request('GET', *args, **kwargs)
    return (yield from response.text())


@asyncio.coroutine
def downlaod_html(p):
    sem = asyncio.Semaphore(1000)
    with (yield from sem):
        print(main_url + p[2])
        page = yield from get(p[2])
        html = parse_goods(page)
        print('ok')
        # for d in html:
        #     sql.insert(p[0, p[1], url, p[2], d[3], d[2], main_url + d[0], d[4])


def collect_urls():
    urls = []
    for name, url in maincats:
        # print(name, url)
        page = requests.get(url).content.decode('utf8')
        soup = BeautifulSoup(page, 'html.parser')
        for suburl, subname in get_suburl(soup):
            suburl = main_url + '/' + suburl
            # print(subname, suburl)
            for num in range(1, 51):
                u = suburl + '?onStock=1&page=' + str(num)
                urls.append((name, subname, u))
    print(len(urls))
    print(urls[0])
    return urls

def run_schedule_upd():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    print('Запускаю парсер')
    f = asyncio.wait([downlaod_html(d) for d in collect_urls()])
    loop = asyncio.get_event_loop()
    loop.run_until_complete(f)



run_schedule_upd()

