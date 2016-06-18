import requests
import sqlite3
from lxml import etree
import grequests

class SQL:
    def __init__(self, test_db):
        self.connection = sqlite3.connect(test_db, check_same_thread=False)
        self.cursor = self.connection.cursor()

    def create_table(self):
        self.table = 'CREATE TABLE data (ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, MAIN_CATEGORY TEXT, SUBCAT TEXT, SUBSUBCAT TEXT, NAME TEXT, ' \
                     'PRODUCT_TYPE TEXT, VENDOR TEXT, MODEL TEXT, DESCRIPTION TEXT, PRICE TEXT, URL TEXT, PIC_URL TEXT)'
        self.cursor.execute(self.table)

    def insert(self, main_category, subcat, subsubcat, prod_type, vendor, model, descr, price, url, pic_url):
        query = 'INSERT into data VALUES ("{}", "{}", "{}", "{}", ' \
                '"{}", "{}", "{}", "{}", "{}", "{}", "{}")'.format(main_category, subcat, subsubcat, prod_type, vendor, model, descr, price, url, pic_url)
        self.cursor.execute(query)
        self.connection.commit()


main_url = 'https://marketmio.ru/'

maincats = [('Компьютерная техника', 'https://marketmio.ru/category/91009'),
            ('Электроника', 'https://marketmio.ru/category/198119'),
            ('Авто', 'https://marketmio.ru/category/90402'),
            ('Детские товары', 'https://marketmio.ru/category/90764'),
            ('Бытовая техника', 'https://marketmio.ru/category/198118')]

urls = [
    'http://www.heroku.com',
    'http://python-tablib.org',
    'http://httpbin.org',
    'http://python-requests.org',
    'http://fakedomain/',
    'http://kennethreitz.com'
]

rs = (grequests.get(u) for u in urls)
for i in rs:
    print(i.)