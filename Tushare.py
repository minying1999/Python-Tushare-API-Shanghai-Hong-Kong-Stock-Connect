import tushare as ts
import Tushare_Token as tt
from pandas import DataFrame
import pandas as pd
import time
import openpyxl
from Date_range_pro import Date
from pandas import DataFrame
import pandas as pd
import time
import openpyxl
#激活tushare接口
ts.set_token(tt.token)
pro = ts.pro_api()

#设置几个日期
#print(Date.date_range('2020-01-01', '2021-01-05',freq="m", out_format="%Y%m%d"))
date_range = Date.date_range('20141117', '20210105',freq="d", out_format="%Y%m%d")
date_range2= Date.date_range('20160629', '20210105',freq="d", out_format="%Y%m%d")
date_range_cf = Date.date_range('20141117','20210105',freq="d", out_format="%Y%m%d")
date_range_sht = Date.date_range('20160629', '20210105',freq="d", out_format="%Y%m%d")
date_range_szt = Date.date_range('20161205', '20210105',freq="d", out_format="%Y%m%d")
date_range_hkt = Date.date_range('2017-3-20', '2021/01/05',freq="d", out_format="%Y%m%d")
date_range_allhold = Date.date_range('20161205', '20210105',freq="d", out_format="%Y%m%d")
#date_range
#1.1  下面是获取[沪深港通资金流向]的代码
df_cf = pd.DataFrame()
for i in date_range_cf:
    print(i)
    df_cf= pd.concat([df_cf,pro.query('moneyflow_hsgt', trade_date=i)],axis=0,ignore_index=True)
    time.sleep(0.5)
##保存为txt
path1 =  r"C:\Users\MY\Python学习\Python金融数据挖掘_余老师\深沪港通数据\沪深港通资金流向\沪深港通资金流向20141117_20200105."
df_cf.to_csv(path1+'txt')
#df_cf.to_csv(path1+'csv')#保存为csv，不需要索引行可以改为df_cf.to_csv(path1+'csv',index=True,header=True)
#df_cf.to_excel(path1+'xlsx')#保存为excel


#1.2  下面是获取[沪深股通十大成交股]的代码
df_bigten = pd.DataFrame()
for i in date_range:
    print(i)
    df_bigten= pd.concat([df_bigten,pro.hsgt_top10(trade_date=i, market_type='1')],axis=0,ignore_index=True)
    time.sleep(0.5)
print(df_bigten)
path2 = r"C:\Users\MY\Python学习\Python金融数据挖掘_余老师\深沪港通数据\沪深股通十大成交股\沪深股通十大成交股_单日."
df_bigten.to_csv(path2+'txt',index=True,header=True)
#df_bigten.to_excel(path2+'xlsx',index=True,header=True)


#1.3  下面是获取[沪深港股通持股明细]的代码
##1.3.1  获取单日全部持股

##获取单日全部持股
#df_hk_hold= pro.hk_hold(trade_date='20190625')
df_all_hold = pd.DataFrame()
for i in date_range_allhold :
    print("df_all_hold" + "已经更新到" + i)
    df_all_hold = pro.hk_hold(trade_date=i)
    print(df_all_hold)
    name = r"C:\Users\MY\Python学习\Python金融数据挖掘_余老师\深沪港通数据\深港股通持股明细\沪深港股通单日全部持股_txt版本\单日总持股明细" + str(i) + '.txt'
    df_all_hold.to_csv(name,index=True,header=True)  # 保存为txt版本
    time.sleep(0.1)
#df_all_hold
##获取单个交易所的单日持股
###首先是SH沪股通
df_hk_hold_sh = pd.DataFrame()
for i in date_range_sht:
    print(i)
    df_hk_hold_sh = pro.hk_hold(trade_date=i, exchange='SH')
    name = r"C:\Users\MY\Python学习\Python金融数据挖掘_余老师\深沪港通数据\深港股通持股明细\沪深港股通区分交易所交易所持股明细\SH单日沪深港股通持股明细_txt版本\SH单日沪深港股通持股明细"+str(i)+'.txt'
    df_hk_hold_sh.to_csv(name,index=True,header=True)
    #time.sleep(0.1)
#df_hk_hold_sh
###其次是SZ深股通
df_hk_hold_sz = pd.DataFrame()
for i in date_range_szt:
    print(i)
    df_hk_hold_sz = pro.hk_hold(trade_date=i, exchange='SZ')
    name = r"C:\Users\MY\Python学习\Python金融数据挖掘_余老师\深沪港通数据\深港股通持股明细\沪深港股通区分交易所交易所持股明细\SZ单日沪深港股通持股明细_txt版本\SZ单日沪深港股通持股明细"+str(i)+'.txt'
    df_hk_hold_sz.to_csv(name,index=True,header=True)
    #time.sleep(0.1)
#df_hk_hold_sz
###最后是HK港股通
df_hk_hold_hk = pd.DataFrame()
for i in date_range_hkt:


    print(i)
    df_hk_hold_hk = pro.hk_hold(trade_date=i, exchange='HK')
    name = r"C:\Users\MY\Python学习\Python金融数据挖掘_余老师\深沪港通数据\深港股通持股明细\沪深港股通区分交易所交易所持股明细\HK单日沪深港股通持股明细_txt版本\HK单日沪深港股通持股明细"+str(i)+'.txt'
    df_hk_hold_sz.to_csv(name,index=True,header=True)
    #time.sleep(0.1)
#df_hk_hold_hk