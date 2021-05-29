# Python-Tushare-crawler-Shanghai-Hong-Kong-Stock-Connect

You should only create a new .py named "Tushare_Token" and write ur tushare token as "token" variance at first.

Enter into 沪深港股通数据获取_封装.py or 沪深港股通数据获取_封装.ipynb and run them after u chose the definition one by one 
like
"
get_bigten(start='20141117',
           end=''.join(str(datetime.now())[:10].split('-')),
           filetype='excel')
"
or directly running  
“
get_update_data(start='',end=''.join(str(datetime.now())[:10].split('-')),filetype='excel',exchange = '',key = 0)“
”

If you do nothing ,the the end_date is today and you can chose params free, too.

The start date that information can be obtained has saved in codes, and they are easy to get, though i had set them as default params.
