for url, cat in arr:
    query_res = sql.execute('select * from data_auto where product_category = "{}"'.format(cat))
    if query_res == None:
        print(url, cat)
