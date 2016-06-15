import re
import requests
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

sql = SQL('hotels_server2.db')


def compile_countries():
    d = {}
    id = 1
    countries = set(sql.search('SELECT COUNTRY FROM booking'))
    for country in sorted(countries, key=lambda x: x[0][0]):
        line = '<category id="' + str(id) +'">' + country[0] + '</category>'
        print(line)
        d[country[0]] = id
        id += 1
    return d


def compile_cities():
    f = open('out.txt', 'w', encoding='utf8')
    cities = set(sql.search('SELECT COUNTRY, CITY FROM booking'))
    for city in sorted(cities, key=lambda x: x[0]):
        print(city)
        print(compile_countries()[city[0]], city[1])
        line  = '<category id="0' + str(compile_countries()[city[0]]) + '" parentId="' + str(compile_countries()[city[0]]) + '">' + city[1] +'</category>'
        print(line)
        f.write(line + '\r\n')
    f.close()


def compile_offer():
    f = open('offers.txt', 'w+', encoding='utf8')
    about = sql.search('SELECT COUNTRY, CITY, STARS, HOTEL, URL FROM booking')
    id = 1
    for hotel in about[201:400]:
        print(str(id) + '---' + str(len(about)))
        line = '<offer available="false" id="' + str(id) + '"><categoryId type="Own">0' + str(compile_countries()[hotel[0]]) + '</categoryId><market_category>Отели</market_category>' \
               '<name>' + hotel[3] + '</name><param name="level">' + hotel[2] + '</param><param name="city">' + hotel[1] + '</param>' \
               '<url>' + hotel[4] + '</url>'
        f.write(line + '\r\n')
        id += 1
    f.close()


def compile_offer1():
    f = open('offers.txt', 'w', encoding='utf8')
    about = sql.search('SELECT COUNTRY, CITY, STARS, HOTEL, URL FROM booking')
    id = 1
    for hotel in about[:200]:
        print(str(id) + '---' + str(len(about)))
        line = '<offer available="false" id="' + str(id) + '"><categoryId type="Own">0' + str(compile_countries()[hotel[0]]) + '</categoryId><market_category>Отели</market_category>' \
               '<name>' + hotel[3] + '</name><param name="level">' + hotel[2] + '</param><param name="city">' + hotel[1] + '</param>' \
               '<url>' + hotel[4] + '</url>'
        f.write(line + '\r\n')
        id += 1
    f.close()

d = {}
c = 19
for country in sql.search('SELECT DISTINCT COUNTRY FROM booking'):
    d[country[0]] = c
    c += 1


with open('hotels_correct2.xml', 'a', encoding='utf8') as fw:
    count = 386777
    for country, city, stars, hotel, url in sql.search('SELECT * FROM booking'):
        line2 = '\t\t\t<offer available="false" id="{}"><categoryId type="Own">{}</categoryId>' \
            '<market_category>Отели</market_category><name>{}</name>' \
            '<param name="level">{}</param><param name="city">{}</param>' \
            '<url>{}</url></offer>\r\n'.format(count, d[country], hotel, stars, city, url)
        count += 1
        fw.write(line2)

    a = '\t\t</offers>\r\n\t</shop>\r\n</yml_catalog>'
    fw.write(a)