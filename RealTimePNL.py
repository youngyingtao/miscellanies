# -*- coding: utf-8 -*-
"""
Created on Thu Feb 21 01:23:01 2019


计算当天的PNL
15点收盘时保存收盘价文件
需要当前持仓，当前成交记录，最新价格，三个文件
20190416更新：将收盘ONL保存到CSV中,打印输出显示优化
20190328更新：解决读到空价格文件问题

@author: yangyingtao
"""
import os
import math
import time
import shutil
import requests
import traceback
import threading
import numpy as np
import pandas as pd
from tabulate import tabulate
from datetime import datetime


class RealTimePNL():
    def __init__(self):
        self.today = datetime.today().strftime("%Y%m%d")
        self.df_config = pd.read_csv("config.csv",dtype={"account_id": str}, encoding="gbk")
        self.sleep_time = 60 #计算PNL时间间隔
        self.sleep_min = 2  # 秒
        self.sleep_max = 4  # 秒
#        self.timer_wind = 1505  # 生成上传wind文件的定时器
#        self.timer_trade_start = 925  # 交易开始时间
#        self.timer_trade_end = 1505  # 交易结束时间
        self.timer_over = 1510  #程序结束时间
        #实时计算PNL线程
        self.calculate_pnl_thread = threading.Thread(target=self.__threadfunc_calculate_pnl)
        self.calculate_pnl_thread.setDaemon(True)
        self.calculate_pnl_thread.start()
        self.mylock = threading.RLock() #锁

    def __threadfunc_calculate_pnl(self):
        """
        计算PNL线程函数
        :return:
        """
        while True:
            self.GetPNL()
            time.sleep(self.sleep_time)
     
    
    def GetPNL(self):
        timer = datetime.today().strftime("%H%M")
        t1 = datetime.now()
        print("thread runing...",timer)
        strategy_pnl_df = pd.DataFrame(columns=["account_name","strategy_name", "pnl"])
        for index in self.df_config.index:
            account_id = self.df_config.loc[index,"account_id"]
            strategy_id = self.df_config.loc[index, "strategy_id"]
            file_path = self.df_config.loc[index,"file_path"]
            data_path= self.df_config.loc[index,"data_path"]
            posShortable_path = self.df_config.loc[index,"posShortable_path"]
            try:
                
                # 前一收盘价
                filetmp = file_path + "/preprice.csv"
                if not os.path.isfile(filetmp):
                    print(filetmp + " no found!!!!")
                    return
                df_preprice = pd.read_csv(filetmp, dtype={"stock_code": str,"last_price":float}, encoding="gbk")
                df_preprice.rename(index=str, columns={"ticker": "stock_code", "last_price": "pre_price"},inplace=True)

                # 今最新价
                filetmp = data_path + "/data.csv"
                if not os.path.isfile(filetmp):
                    print(filetmp + " no found!!!!")
                    return
                df_last_price = pd.read_csv(filetmp,names=['stock_code','trade_time','last_price','ask1','bsize1','asize1'],encoding='gbk')
                print('The last tick data count:%s'%df_last_price.shape[0])
                #  如果最新价格数据个数多于3500个，则备份此刻价格数据，否则，仍然使用备份的前最新数据
                if df_last_price.shape[0] > 3500:
                    shutil.copy(filetmp,file_path + '/backupdata.csv')
                else:
                    df_last_price = pd.read_csv(file_path + '/backupdata.csv',names=['stock_code','trade_time','last_price','ask1','bsize1','asize1'],encoding='gbk')
                #  将为0的价格，使用卖一价替代
                for index,last_price in enumerate(df_last_price['last_price']):
                    if last_price == 0:
                        df_last_price.loc[index,'last_price'] = df_last_price.loc[index,'ask1']
                df_lastprice = df_last_price[['stock_code', 'last_price']]
                
                if int(timer)>(self.timer_over-3):
#                    df_new_preprice = df_lastprice.copy()
#                    df_new_preprice.rename(index=str, columns={"stock_code": "stock_code", "last_price": "pre_price"},inplace=True)
                    df_lastprice.to_csv(file_path + '/preprice.csv',index=False)

                
                # 最新总持仓
                filetmp = file_path +'/'+ account_id + ".pos"
                if not os.path.isfile(filetmp):
                    print(filetmp + " no found!!!!")
                    return
                df_lastposition = pd.read_csv(str(filetmp),names=['a','stock_code','account_id','FEX','fee_ratio','1','value','current_open','none','b','c','d'],encoding="gbk")
                df_lastposition = df_lastposition[df_lastposition.account_id.isin([account_id])]

                filetmp = file_path + "/hedge_indexfutures.csv"
                df_hedge_indexfutures = pd.read_csv(filetmp,dtype={"stock_code": str,"base_open":int,"multiplier":int},encoding='gbk')
                # 对冲股票底仓
                filetmp = posShortable_path + "/posShortable.csv"
                df_posShortable = pd.read_csv(filetmp,names=['stock_code','base_open'],encoding='gbk')
                df_posShortable = pd.concat([df_posShortable,df_hedge_indexfutures],axis=0,sort=False).fillna(1)

                df_TradingData = df_posShortable.merge(df_lastposition,on='stock_code',how='left').fillna(0)
                df_TradingData = df_TradingData.merge(df_lastprice,on="stock_code",how="left").fillna(0)
                df_TradingData = df_TradingData.merge(df_preprice,on="stock_code",how="left").fillna(0)

                df_TradingData.loc[(df_TradingData["last_price"].isnull()) | (df_TradingData["last_price"] == 0), "last_price"] = df_TradingData["pre_price"]
                df_TradingData.loc[(df_TradingData["pre_price"].isnull()) | (df_TradingData["pre_price"] == 0), "pre_price"] = df_TradingData["last_price"] 
 
                # 今成交
                filetmp ="%s/trade/%s.trade" %(file_path,self.today)
                if not os.path.isfile(filetmp):
                    print(filetmp + " no found!!!!")
                    return
                df_deal = pd.read_csv(filetmp,names=['a','b','account_id','strategy_id','stock_code','direction','oftype','speculation','deal_price','deal_amount','fee','c','d','trade_date','trade_time'],encoding="gbk")
                df_deal = df_deal[df_deal.account_id.isin([account_id]) & df_deal.strategy_id.isin([strategy_id])]

                # 计算PNL
                strategy_pnl,PNLDetail = self.CalculateTodayPNL(strategy_id,df_TradingData,df_deal)
                strategy_pnl_df = strategy_pnl_df.append(pd.DataFrame(dict(account_name=account_id,strategy_name=strategy_id, pnl=strategy_pnl),index=[1]), ignore_index=True)

                if int(timer) > (self.timer_over - 1):
                    #  收盘PNL记录留存在CSV文件中
                    PNLDetail.insert(0, datetime.today().strftime("%Y.%m.%d"))
                    df_PNLDetail = pd.DataFrame([PNLDetail], columns=['date', 'Strategy_id', 'T0_pnl', 'Base_pnl', 'IC_pnl', 'Commission', 'Total_pnl'])
                    df_PNLDetails = pd.read_csv(file_path + '/' + strategy_id + "_PNLDetails.csv", encoding="gbk")
                    df_PNLDetails = df_PNLDetails.append(df_PNLDetail,ignore_index=True)
                    df_PNLDetails.to_csv(file_path + '/' + strategy_id + "_PNLDetails.csv", index=False)


            except: 
                traceback.print_exc()
#        SendPnl(strategy_pnl_df)
        t2 = datetime.now()
        print("----------------------elapsed time:%s----------------------"%(t2 - t1))



    def CalculateTodayPNL(self,strategy_id,df_TradingData,df_deal):
        try:
            fee_rate = 0.0007
            df_commission = pd.DataFrame(columns=('stock_code','commission'))
            df_today_open = pd.DataFrame(columns=('stock_code','today_open'))
            df_trading_pnl = pd.DataFrame(columns=('stock_code','trading_pnl'))
            for index in df_TradingData.index:
                today_open = 0
                trading_pnl = 0
                commission = 0
                stock_code = df_TradingData.loc[index,'stock_code']
                last_price = df_TradingData.loc[index,'last_price']
                
                df_singel_deal = df_deal.loc[df_deal['stock_code'] == stock_code]

                if df_singel_deal.empty:
                    continue
                else:
                    for index in df_singel_deal.index:
                        direction = df_singel_deal.loc[index,'direction']
                        deal_amount = df_singel_deal.loc[index,'deal_amount']
                        deal_price = df_singel_deal.loc[index,'deal_price']
                        if direction == 'Buy':
                            pos_change = deal_amount
                        else:
                            pos_change = -deal_amount
                            
                        #单笔成交金额
                        turnover = deal_price * deal_amount
                        #累计手续费
                        commission += turnover * fee_rate

                        trading_pnl += pos_change * (last_price - deal_price)
                        today_open += pos_change

                    df_today_open = df_today_open.append(pd.DataFrame(dict(stock_code=stock_code,today_open=today_open),index=[0]),sort=False)
                    df_trading_pnl = df_trading_pnl.append(pd.DataFrame(dict(stock_code=stock_code,trading_pnl=trading_pnl),index=[0]),sort=False)
                    df_commission = df_commission.append(pd.DataFrame(dict(stock_code=stock_code,commission=commission),index=[0]),sort=False)

            df_TradingData['hedge_sigpnl'] = df_TradingData['base_open'] * (df_TradingData["last_price"] - df_TradingData["pre_price"]) * df_TradingData["multiplier"]
            df_TradingDataIC = df_TradingData[(df_TradingData['stock_code'].isin(['IC1906.CFFEX']))]
            df_TradingDataBase = df_TradingData[~(df_TradingData['stock_code'].isin(['IC1906.CFFEX']))]
#            hedge_pnl = int(df_TradingData['hedge_sigpnl'].sum())
            IC_pnl = int(df_TradingDataIC['hedge_sigpnl'].sum())
            Base_pnl = int(df_TradingDataBase['hedge_sigpnl'].sum())
            hedge_pnl = IC_pnl + Base_pnl


            df_TradingData2 = df_TradingData.copy()
            df_TradingData2 = df_TradingData2.merge(df_commission,on='stock_code',how='left').fillna(0)
            df_TradingData2 = df_TradingData2.merge(df_today_open,on='stock_code',how='left').fillna(0)
            df_TradingData2 = df_TradingData2.merge(df_trading_pnl,on='stock_code',how='left').fillna(0)
            df_TradingData2 = df_TradingData2[~(df_TradingData2['stock_code'].isin(['IC1906.CFFEX']))]
            df_TradingData2["yes_open"] = df_TradingData2["current_open"] - df_TradingData2["base_open"] - df_TradingData2["today_open"]
            df_TradingData2['yes_open_sigpnl'] = df_TradingData2["yes_open"] * (df_TradingData2["last_price"] - df_TradingData2["pre_price"]) * df_TradingData2["multiplier"]
            yes_open_pnl = int(df_TradingData2['yes_open_sigpnl'].sum())
            trading_pnl = int(df_TradingData2['trading_pnl'].sum())
            commission = int(df_TradingData2['commission'].sum())

            #当天股票T+0盈亏
            if math.isnan(trading_pnl):
                trading_pnl = 0
            if math.isnan(yes_open_pnl):
                yes_open_pnl = 0

            t0_pnl = yes_open_pnl + trading_pnl - commission          
            total_pnl = t0_pnl + hedge_pnl
            PNLDetail = [t0_pnl,Base_pnl,IC_pnl,commission,total_pnl]
            PNLDetail.insert(0,strategy_id)
            print(tabulate([PNLDetail],headers=['Strategy_id','T0_pnl','Base_pnl','IC_pnl','Commission','Total_pnl'],tablefmt="orgtbl"))
            return t0_pnl,PNLDetail
        except:
            traceback.print_exc()

                 
def SendPnl(strategy_pnl_df):
    strategy_pnl_df = strategy_pnl_df.fillna(0.0)
    groups = dict()
    account_dict = {"110002398577":"泰","41900046138":"平稳","666800005765":"南粤","666800000587":"康乾","666800011923":"扬2","0025359638":"红上","001616119199":"伏尔加","21698075":"扬2光大"}
    prop_dict = {"110002398577": 3100000, "41900046138":1000000,"666800005765": 1000000, "666800011923": 1000000, "666800000587": 18600000,"0025359638": 5700000,"001616119199": 4200000, "21698075": 2100000}
    cloudUrl = "http://115.159.37.177:3000/accountmonitor"
    for index in strategy_pnl_df.index:
        account_name = strategy_pnl_df.loc[index, "account_name"]
        strategy_name = strategy_pnl_df.loc[index, "strategy_name"]
        #strategy_name = account_name
        key = str(account_name)
        pnl = strategy_pnl_df.loc[index, "pnl"]
        if float(abs(pnl)) > 200000:
            print("pnl too high!!!!, pnl is:", pnl)
            #os.system("pause")
        groups[key] = dict()
        groups[key]['a'] = str(account_name)             #账户名
        account_name2 = ""
        for (k,v) in account_dict.items():
            if k == str(account_name):
                account_name2 = v
        groups[key]['nm'] = str(account_name2) + "_" + "Y"              #策略名
        groups[key]['pbl'] = prop_dict[key]
#        print(2)
#        print(type(groups[key]['pbl']))
        groups[key]['dp'] = 0
        groups[key]['wd'] = 0
        groups[key]['bl'] = float(pnl + groups[key]['pbl'])  
#        print(3)
#        print(type(groups[key]['pbl']))
        groups[key]['ss'] = 0
    para = {}
    para['time'] = time.time() * 1000
    para['from'] = 'stock_t0'
    try:
      res = requests.post(cloudUrl, params=para, json=groups)
#      print(groups)
    except Exception as e:
      print(e)
                
def printpy(outdata):
    if outdata.ErrorCode!=0:
        print('error code:'+str(outdata.ErrorCode)+'\n');
        return();
    for i in range(0,len(outdata.Data[0])):
        strTemp=''
        if len(outdata.Times)>1:
            strTemp=str(outdata.Times[i])+' '
        for k in range(0, len(outdata.Fields)):
            strTemp=strTemp+str(outdata.Data[k][i])+' '
        print(strTemp)

if __name__ == "__main__":
    timer = datetime.today().strftime("%H%M")
    realtimePNL = RealTimePNL()
    while True:
        if int(timer) > realtimePNL.timer_over:
            break
        timer = datetime.today().strftime("%H%M")
