# -*- coding: utf-8 -*-


import requests
import sqlite3
import re





class SQL:
    def __init__(self, test_db):
        self.connection = sqlite3.connect(test_db, check_same_thread=False)
        self.cursor = self.connection.cursor()

    def create_table(self):
        self.table = 'CREATE TABLE data_tehnika (MAIN_CATEGORY TEXT, PRODUCT_CATEGORY TEXT, PRODUCTS_PAGE TEXT, ' \
                     'FULLNAME TEXT, DESCRIPTION TEXT, PRICE TEXT, URL TEXT, PIC_URL TEXT)'
        self.cursor.execute(self.table)

    def insert(self, main_category, subcat, name, fullname, descr, price, url, pic_url):
        query = 'INSERT into data_tehnika VALUES ("{}", "{}", "{}", "{}", ' \
                '"{}", "{}", "{}", "{}")'.format(main_category, subcat, name, fullname, descr, price, url, pic_url)
        self.cursor.execute(query)
        self.connection.commit()

    def execute(self, query):
        self.cursor.execute(query)
        self.connection.commit()


sql = SQL('marketmio.db')
try:
    sql.create_table()
except:
    pass

main_url = 'https://marketmio.ru'

maincats = [
            # ('Компьютерная техника', 'https://marketmio.ru/category/91009'),
            # ('Электроника', 'https://marketmio.ru/category/198119')
            # ('Авто', 'https://marketmio.ru/category/90402'),
            # ('Детские товары', 'https://marketmio.ru/category/90764'),
            ('Бытовая техника', 'https://marketmio.ru/category/198118')
          ]


regex_produrl = re.compile('<a class="product__image-container ?e?v?e?n?t?" href="(.*?)"', re.M)
regex_fullname = re.compile('<a class="product__name-link ?e?v?e?n?t?" href=".*?".*? title=".*?">(.*?)</a>', re.M)
regex_price = re.compile('">(.*?)\sруб', re.M | re.IGNORECASE)
regex_description = re.compile('<a class="product__description.*?">\s*(.*?)\s.*?<ul class="bullet-list">', re.M)
regex_pic_url = re.compile('src="(.*?jpe?g)', re.IGNORECASE | re.M)



def get_suburl(page):
    res = re.findall('<a href="(.*?)".*?"select-category__link link">(.*?)</a>', page)
    print(res)
    return res





def collect_urls():
    for name, url in maincats:
        print(name, url)
        page = requests.get(url).content.decode('utf8')
        for suburl, subname in get_suburl(page)[2:]:
            suburl = main_url + '/category/12327183'
            print(suburl, subname)
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

# collect_urls()


#TODO: subparse | auto end
# /category/12327183 Аккумуляторы
# /category/6499811 Автосвет
# /category/12327177 Фильтры
# /category/7766179 Экипировка и защита
# /category/6269371 Видеорегистраторы
# /category/90462 Радар-детекторы
# /category/4317343 Устройства громкой связи
# /category/90478 Моторные масла
# /category/90417 Автомобильные телевизоры
# /category/90404 Автомагнитолы
# /category/308016 Автоакустика
# /category/12327158 Обустройство салона
# /category/4953538 Защита и внешний тюнинг
# /category/13208016 Багажные системы
# /category/12327172 Необходимый набор автомобилиста
# /category/12327203 Инвентарь для ухода