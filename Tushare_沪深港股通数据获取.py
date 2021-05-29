#!/usr/bin/env python
# coding: utf-8

import Tushare_Token as tt
import tushare as ts
import pandas as pd
import threading
import openpyxl
import time
from Date_range_pro import Date
from datetime import  datetime
#激活tushare接口
ts.set_token(tt.token)
pro = ts.pro_api()

#设置几个日期，后面合并到函数封装内
#print(Date.date_range('2020-01-01', '2021-01-05',freq="m", out_format="%Y%m%d"))
#date_range = Date.date_range('20141117', '20210105',freq="d", out_format="%Y%m%d")
#date_range2= Date.date_range('20160629', '20210105',freq="d", out_format="%Y%m%d")
#date_range_cf = Date.date_range('20141117','20210105',freq="d", out_format="%Y%m%d")
#date_range_bigten = date_range_cf
#date_range_sht = Date.date_range('20160629', '20210105',freq="d", out_format="%Y%m%d")
#date_range_szt = Date.date_range('20161205', '20210105',freq="d", out_format="%Y%m%d")
#date_range_hkt = Date.date_range('2017-3-20', '2021/01/05',freq="d", out_format="%Y%m%d")
#date_range_allhold = Date.date_range('20161205', '20210105',freq="d", out_format="%Y%m%d")
#date_range




# In[2]:


#1.1  下面是获取[沪深港通资金流向]的代码
def get_cashflow(start='20141117', end='20210105', filetype='csv'):
    time_initial  = time.process_time ()
    print('资金流向会合并成一个文件')
    date_range_cf = Date.date_range(start, end, freq="d", out_format="%Y%m%d")
    df_cf = pd.DataFrame()
    count = 0
    for i in date_range_cf:
        time_strat = time.process_time()
        #print(i)
        #print("df_cf（沪深港通资金流向）已经处理到" + i)
        df_cf = pd.concat(
            [df_cf, pro.query('moneyflow_hsgt', trade_date=i)],
            axis=0,
            ignore_index=True)
        time.sleep(0.5)
        time_end = time.process_time()
        print('正在合并{}文件，单次循环时长为{}秒'.format(i, -(time_strat - time_end)))
        count += 1
        print('目前已经合并{}个，进度为{:.3%}'.format(count, count / len(date_range_cf)))
    path_cashflow = r"H:\数据备份\沪深港股通数据\沪深港通资金流向" + str(start) + 'to' + str(end)
    if filetype == 'txt'or filetype =='csv':
        df_cf.to_csv(path_cashflow + '.txt',encoding = 'utf-8',index = False)
    elif filetype == 'excel':
        df_cf.to_excel(path_cashflow + '.xlsx',encoding = 'utf-8',index = False)

    time_final = time.process_time ()
    print("——————————————————————————————————————")
    print("总共的运行时间是:{:.5}秒".format(-(time_initial-time_final)))
    print('沪深港通资金流向写入完成，路径为{}'.format(path_cashflow))
    #df_cf.to_csv(path_cashflow+'csv')#保存为csv，不需要索引行可以改为df_cf.to_csv(path_cashflow+'csv',index=True,header=True)
    #df_cf.to_excel(path_cashflow+'xlsx')#保存为excel


# In[3]:


#1.2  下面是获取[沪深股通十大成交股]的代码
def get_bigten(start='20141117', end='20210105', filetype='csv'):
    time_initial = time.process_time()
    print("十大成交股会合并成为一个文件")
    count = 0
    df_bigten_sh = pd.DataFrame()
    df_bigten_sz = pd.DataFrame()
    date_range_bigten = Date.date_range(start,
                                        end,
                                        freq="d",
                                        out_format="%Y%m%d")
    path_bigten = r"H:\数据备份\沪深港股通数据\沪深股通十大成交股_单日" + str(start) + 'to' + str(
        end)
    #下面是上证
    for i in date_range_bigten:
        time_strat = time.process_time()
        print("df_bigten_sh已经处理到" + i)
        data_get_sh = pro.hsgt_top10(trade_date=i, market_type='1')
        time.sleep(0.2)
        data_get_sh = data_get_sh.sort_values(by="rank", ascending=True)
        df_bigten_sh = pd.concat([df_bigten_sh, data_get_sh],
                                 axis=0,
                                 ignore_index=True)
        time_end = time.process_time()
        print('正在合并{}文件，单次循环时长为{}秒'.format(i, -(time_strat - time_end)))
        count += 1
        print('目前已经合并{}个，进度为{:.3%}，耗时{}s'.format(count, count / len(date_range_bigten),-(time_initial - time_end)))
    if filetype == 'txt' or filetype == 'csv':
        df_bigten_sh.to_csv(path_bigten + 'sh.' + filetype,
                            header=True,
                            encoding='utf-8',
                            index=False)
    elif filetype == 'excel':
        df_bigten_sh.to_excel(path_bigten + 'sh.' + 'xlsx',
                              header=True,
                              encoding='utf-8',
                              index=False)
        print('后缀为.xlsx')

    #下面是深证
    count = 0
    for i in date_range_bigten:
        time_strat = time.process_time()
        time.sleep(0.2)
        print("df_bigten_sz已经处理到" + i)
        data_get_sz = pro.hsgt_top10(trade_date=i, market_type='3')
        data_get_sz = data_get_sz.sort_values(by="rank", ascending=True)
        df_bigten_sz = pd.concat([df_bigten_sz, data_get_sz],
                                 axis=0,
                                 ignore_index=True)
        time_end = time.process_time()
        print('正在生成{}文件，单次循环时长为{}秒'.format(i, -(time_strat - time_end)))
        count += 1
        print("当前第二阶段处理深证数据")
        print('目前已经生成{}个，进度为{:.3%}，耗时{}s'.format(count, count / len(date_range_bigten),-(time_initial - time_end)))
    if filetype == 'txt' or filetype == 'csv':
        df_bigten_sz.to_csv(path_bigten + 'sz.' + filetype,
                            header=True,
                            encoding='utf-8',
                            index=False)
    elif filetype == 'excel':
        df_bigten_sz.to_excel(path_bigten + 'sz.' + 'xlsx',
                              header=True,
                              encoding='utf-8',
                              index=False)

    time_final = time.process_time()

    print("——————————————————————————————————————")
    print("加上总共的运行时间是:{}秒，路径为{}".format(-(time_initial - time_final),
                                        path_bigten))
    print('十大成交股写入完成')


# In[4]:


#1.3  下面是获取[沪深港股通持股明细]的代码


##获取单日全部持股
#df_hk_hold= pro.hk_hold(trade_date='20190625')
def get_all_hold_oneday(start='20161205', end='20210105', filetype='csv'):
    time_initial = time.process_time()
    df_all_hold = pd.DataFrame()
    count = 0
    date_range_allhold = Date.date_range(start,
                                         end,
                                         freq="d",
                                         out_format="%Y%m%d")
    for i in date_range_allhold:
        print("df_all_hold已经更新到" + i + '，每一天都会生成一个单独的文件')
        time_strat = time.process_time()
        df_all_hold = pro.hk_hold(trade_date=i)
        time.sleep(0.125)
        #print(df_all_hold)
        if filetype == 'txt' or filetype == 'csv':
            path_all_hold = r"H:\数据备份\沪深港股通数据\深港股通持股明细_不区分交易所\沪深港股通单日全部持股_" + filetype + "版本\单日总持股明细" + str(
                i) + '.' + filetype
            df_all_hold.to_csv(path_all_hold,
                               header=True,
                               encoding='utf-8',
                               index=False)  # 保存为txt版本
        elif filetype == 'excel':
            path_all_hold = r"H:\数据备份\沪深港股通数据\深港股通持股明细_不区分交易所\沪深港股通单日全部持股_" + filetype + "版本\单日总持股明细" + str(
                i) + '.xlsx'
            df_all_hold.to_excel(path_all_hold,
                                 header=True,
                                 encoding='utf-8',
                                 index=False)  # 保存为excel版本
        time_end = time.process_time()
        print('正在合并{}文件，单次循环时长为{}秒'.format(i, -(time_strat - time_end)))
        count += 1
        print('目前已经合并{}个，进度为{:.3%}'.format(count,
                                           count / len(date_range_allhold)))
    time_final = time.process_time()
    print("——————————————————————————————————————")
    print("加上总共的运行时间是:{}秒".format(-(time_initial - time_final)))
    print('单日全部持股-按日分 全部写入完成')


# In[5]:


##获取单个交易所的单日持股
###首先是SH沪股通


def get_all_hold_exchange(start='20160629',
                          end='20210105',
                          filetype='csv',
                          exchange='SH'):
    """
    exchange可以为:
    "SZ","SH","HK"
    """
    time_initial = time.process_time()
    df_hk_hold_sh = pd.DataFrame()
    date_range_sht = Date.date_range(start, end, freq="d", out_format="%Y%m%d")
    count = 0
    for i in date_range_sht:
        time_strat = time.process_time()
        time.sleep(0.2)
        print("df_all_hold(按交易所区分)已经更新到" + i)
        df_hk_hold_sh = pro.hk_hold(trade_date=i, exchange=exchange)
        print(type(filetype))
        if filetype == 'txt' or filetype == 'csv':
            path_all_hold = r"H:\数据备份\沪深港股通数据\沪深港股通区分交易所" + "\\" + exchange + "单日沪深港股通持股明细_" + filetype + "版本\\" + exchange + "单日沪深港股通持股明细" + str(
                i) + '.' + filetype
            df_hk_hold_sh.to_csv(path_all_hold,
                                 header=True,
                                 encoding='utf-8',
                                 index=False)
        elif filetype == 'excel':
            path_all_hold = r"H:\数据备份\沪深港股通数据\沪深港股通区分交易所" + "\\" + exchange + "单日沪深港股通持股明细_" + filetype + "版本\\" + exchange + "单日沪深港股通持股明细" + str(
                i) + '.xlsx'
            df_hk_hold_sh.to_excel(path_all_hold,
                                   header=True,
                                   encoding='utf-8',
                                   index=False)
        else:
            print('filetype错误')
        time_end = time.process_time()
        print('正在写入{}{}文件，单次循环时长为{:.5}秒'.format(exchange + str(i), filetype,
                                                -(time_strat - time_end)))
        count += 1
        print('目前已经写入{}个，进度为{:.3%}'.format(count, count / len(date_range_sht)))
        print("当前总耗时{:.5}".format(-(time_initial - time_end)))
    time_final = time.process_time()
    print("——————————————————————————————————————")
    print("总共的运行时间是:{:.5}秒".format(-(time_initial - time_final)))
    print('单日全部持股{}-按交易所分 全部写入完成'.format(exchange))


# 下面开始获取

# In[6]:


def get_old_data(start, end, filetype, exchange):
    get_cashflow(start='20141117', end='20210105', filetype='excel')
    get_bigten(start='20141117', end='20210105', filetype='excel')
    #get_all_hold_exchange(start = '20160629', end = '20210105', filetype = 'csv', exchange = 'SH')
    get_all_hold_exchange(start='20160629',
                          end='20210105',
                          filetype='excel',
                          exchange='SH')
    #get_all_hold_exchange(start = '20161205', end = '20210105', filetype = 'csv', exchange = 'SZ')
    get_all_hold_exchange(start='20161205',
                          end='20210105',
                          filetype='excel',
                          exchange='SZ')
    #get_all_hold_exchange(start = '20170320', end = '20210105', filetype = 'csv', exchange = 'HK')
    get_all_hold_exchange(start='20170320',
                          end='20210105',
                          filetype='excel',
                          exchange='HK')
    get_all_hold_oneday(start='20161205', end='20210105', filetype='excel')


# In[7]:


def get_update_data(start, end, filetype, exchange, key=1):
    judge = print("沪深股通资金流向和十大成交股由于需要合并成单文件，因而需要重新重头获取，速度较慢")
    print('默认不更新，更新请更改key为0')
    start = input('请以8位数字的形式输入要开始更新的日期，输入1退出')
    today = ''.join(str(datetime.now())[:10].split('-'))
    if start == 1:
        return
    else:
        print('默认更新到今日{}'.format(today))
        #get_all_hold_exchange(start = start, end = today, filetype = filetype, exchange = 'SH')
        get_all_hold_exchange(start=start,
                              end=today,
                              filetype=filetype,
                              exchange='SH')
        #get_all_hold_exchange(start = start, end = today, filetype = filetype, exchange = 'SZ')
        get_all_hold_exchange(start=start,
                              end=today,
                              filetype=filetype,
                              exchange='SZ')
        #get_all_hold_exchange(start = start, end = today, filetype = filetype, exchange = 'HK')
        get_all_hold_exchange(start=start,
                              end=today,
                              filetype=filetype,
                              exchange='HK')
        get_all_hold_oneday(start=start, end=today, filetype=filetype)
        print("沪深股通资金流向和十大成交股由于需要合并成单文件，因而需要重新重头获取，速度较慢")
        print('确认一次，更新沪深股通资金流向和十大成交股请更改key为0')
        if key == 0:
            print('沪深股通资金流向和十大成交股正在从2014117开始更新')
            get_cashflow(start='20141117', end=today, filetype=filetype)
            get_bigten(start='20141117', end=today, filetype=filetype)
        elif key == 1:
            print('确认一次，本次沪深股通资金流向和十大成交股没有进行更新')


# In[8]:


#get_update_data(start='',end=''.join(str(datetime.now())[:10].split('-')),filetype='excel',exchange = '',key = 0)


# In[ ]:


#get_bigten(start='20141117',end=''.join(str(datetime.now())[:10].split('-')), filetype='excel')


# In[ ]:




