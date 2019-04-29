# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 16:14:20 2019

@author: todd
"""

import csv
import pymongo
import pandas as pd
import tushare as ts
#from WindPy import w

#client = pymongo.MongoClient(host='localhost',port=27017)
#db = client.todd
#col = db.stockdata
#
#def insertToMongoDB(collection):
#    with open('stock_daily_data.csv','r') as csvfile:
#        reader = csv.DictReader(csvfile)
#        counts = 0
#        for each in reader:
#            collection.insert(each)
#            counts += 1
#            print('成功添加了%s条数据'%counts)
#            
#insertToMongoDB(col)

#with open('stock_daily_data.csv','r') as csvfile:
#    reader = csv.DictReader(csvfile)
#    print(reader)
#    counts = 0
#    for each in reader:
#        print(each)
#        col.insert(each)
#        counts += 1
#        print('成功添加了%s条数据'%counts)
symbol = '300136.SZ'
Start_Date = '20100101'
End_Date = '20110410'
pro = ts.pro_api()
data = pro.daily(ts_code=symbol,start_date=Start_Date,end_date=End_Date)
#data2 = ts.get_hist_data('300136',start='2019-01-01',end='2019-04-10')
#data3 = ts.get_k_data('300136',start='2019-01-01',end='2019-04-10')