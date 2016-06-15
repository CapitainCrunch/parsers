__author__ = 'Bogdan'
__email__ = 'evstrat.bg@gmail.com'
__mobile_phone__ = 89252608400

import requests
import re
import sqlite3


class SQL:
    def __init__(self, test_db):
        self.connection = sqlite3.connect(test_db, check_same_thread=False)
        self.cursor = self.connection.cursor()

    def create_table_booking(self):
        self.table = 'CREATE TABLE booking (COUNTRY TEXT, CITY TEXT, STARS TEXT, HOTEL TEXT, URL TEXT)'
        self.cursor.execute(self.table)

    def insert(self, country, city, stars, hotel, url):
        query = 'INSERT into booking VALUES ("{}", "{}", "{}", "{}", "{}")'.format(country, city, stars, hotel, url)
        self.cursor.execute(query)
        self.connection.commit()

    def search_all(self, table):
        query = 'SELECT * FROM "{}"'.format(table)
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def search(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

def compile_country_page(country_url):
    url = 'http://www.booking.com/destination/country/' + country_url + '.html?label=gen173nr-15' \
            'CAEoggJCAlhYSDNiBW5vcmVmaMIBiAEBmAEhuAEEyAEE2AED6AEB;sid=d99dc00a88a0538a48914b75804c8a8d;dcid=4'
    return url


def compile_city_page(country, city):
    url = 'http://www.booking.com/destination/city/' + country + '/' + city + '.html?label=gen173nr-15' \
            'CAEoggJCAlhYSDNiBW5vcmVmaMIBiAEBmAEhuAEEyAEE2AED6AEB;sid=d99dc00a88a0538a48914b75804c8a8d;dcid=4'
    return url


def compile_hotels_pages(city, hotel):
    url = 'http://www.booking.com/hotel/' + city + '/' + hotel + '.html?label=gen173nr-15CAEoggJCAlhYSDNiBW5vcmVmaMIBiAEBmAEhuAEEyAEE2AED6AEB;sid=d99dc00a88a0538a48914b75804c8a8d;dcid=4'
    return url


main_url = 'http://www.booking.com/destination.ru.html?label=gen173nr-15CAEoggJCAlhYSDNiBW5vcmVmaMIBiAE' \
           'BmAEhuAEEyAEE2AED6AEB;sid=d99dc00a88a0538a48914b75804c8a8d;dcid=4'

page = requests.get(main_url)
country = re.findall('<a href="/destination/country/(.*?).html?.*?>(.*?)</a>', page.content.decode('utf8'), flags=re.DOTALL)
hotels_stars_regex = re.compile('<tr><td colspan="2"><h4>\n(.*?)\n</h4>', flags=re.DOTALL)
hotels_regex = re.compile('<a href="/hotel/(.*?)/(.*?).html">(.*?)</a><br />', flags=re.DOTALL)
city_regex = re.compile('<a href="/destination/city/.*?/(.*?).html".*?>(.*?)</a><br />', flags=re.DOTALL)


sql = SQL('./hotels_server.db')
try:
    sql.create_table_booking()
except:
    pass

def crawl():
    for k, v in country[37:]:
        print(v)
        country_p = requests.get(compile_country_page(k))
        cities = city_regex.findall(country_p.content.decode('utf8'))
        print(len(cities))
        pos = 1
        for x,y in cities:
            print('Осталось ' + str(len(cities) - pos))
            cities_p = requests.get(compile_city_page(k.split('.')[0], x))
            hotels_stars = hotels_stars_regex.findall(cities_p.content.decode('utf8'))
            for i in hotels_stars:
                hotels = hotels_regex.findall(cities_p.content.decode('utf8'))
                for c,h, n in hotels:
                    hotel = requests.get(compile_hotels_pages(c + '.ru', h))
                    sql.insert(v, y, i, n, compile_hotels_pages(c, h))
            pos += 1



