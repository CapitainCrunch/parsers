import requests
import sqlite3
from bs4 import BeautifulSoup
import re

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


regex_produrl = re.compile('<a class="product__image-container ?e?v?e?n?t?" href="(.*?)"', re.M)
regex_fullname = re.compile('<a class="product__name-link ?e?v?e?n?t?" href=".*?".*? title=".*?">(.*?)</a>', re.M)
regex_price = re.compile('>?.*?(\d.*?)\sруб.')
regex_description = re.compile('<a class="product__description.*?">\s*(.*?)\s.*?<ul class="bullet-list">', re.M)
regex_pic_url = re.compile('src="(.*?jpe?g)', re.IGNORECASE | re.M)



def get_suburl(soup_obj):
    cats = []
    for link in soup_obj.find_all('a'):
        if link.get('class') == ['select-category__link', 'link']:
            res, = re.findall('<a class="select-category__link link" href="/(.*?)" title="(.*?)">.*?</a>', str(link))
            cats.append(res)
    print(cats)
    return cats


def run_parser():
    for name, url in maincats:
        print(name, url)
        page = requests.get(url).content.decode('utf8')
        soup = BeautifulSoup(page, 'html.parser')
        for suburl, subname in get_suburl(soup)[4:]:
            suburl = main_url + '/' + suburl
            print(subname, suburl)
            for num in range(1, 51):
                u = suburl + '?onStock=1&page=' + str(num)
                print(u)
                prodpage = requests.get(u).content.decode('utf8')
                produrl = []
                fullname = []
                price = []
                description = []
                pic_url = []
                articles = re.findall('<article .*?</article>', prodpage, re.M | re.DOTALL)
                for article in articles:
                    produrl_res = regex_produrl.findall(article)
                    fullname_res = regex_fullname.findall(article)
                    price_res = regex_price.findall(article)
                    description_res = regex_description.findall(article) if len(regex_description.findall(article)) > 0 else [None]
                    pic_url_res = regex_pic_url.findall(article) if len(regex_pic_url.findall(article)) > 0 else [None]
                    if len(pic_url_res) == 0:
                        raise InterruptedError

                    
                    produrl.extend(produrl_res)
                    fullname.extend(fullname_res)
                    price.extend(price_res)
                    description.extend(description_res)
                    pic_url.extend(pic_url_res)
                
                data = zip(produrl, fullname, price, description, pic_url)
                for d in data:
                    print(name, subname, u, d[1], d[3], d[2], main_url + d[0], d[4])
                    # MAIN_CATEGORY, PRODUCT_CATEGORY, PRODUCTS_PAGE, FULLNAME, DESCRIPTION, PRICE, URL, PIC_URL
                    sql.insert(name, subname, u, d[1], d[3], d[2], main_url + d[0], d[4])


run_parser()


