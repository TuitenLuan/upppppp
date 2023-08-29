from clickhouse_driver import Client
import datetime
from datetime import timedelta
import pandas as pd
clickhouse_info = {
        "host": "192.168.20.153",
                "user": "da_team",
                "password": "Ftech@123"
    }
client_ch = Client(host=clickhouse_info['host'], user=clickhouse_info['user'],
                    password=clickhouse_info['password'], settings={
                        'use_numpy': True}
                    )

client_ch.execute('create or replace table da_cdp_funzy.data_retention_mkt(AccountID String,LoginDate Date,regist_time Date,media_source String,Campaign String,Adset String,campaign_type String) ENGINE =MergeTree ORDER BY AccountID')
query="""with data1 as(
select distinct toString(AccountID) as AccountID,LoginDate from da_cdp_funzy.Vw_Account_Login
where GameCode='lcm'),
data8 as(
select user_id,media_source,Campaign,Adset,(case when campaign_type='ua' then 'non-organic' else campaign_type END) as campaign_type,event_time,row_number() over (partition by user_id order by event_time asc) as smt from da_cdp_funzy.appsflyer_user_info),
data9 as(
select user_id,media_source,Campaign,Adset,campaign_type,toDate(event_time) as event_time,smt from data8  where user_id<>'' and smt=1),
data10 as(
select *,first_value(LoginDate) over (partition by AccountID order by LoginDate asc) as first_login from data1 left join data9 on data1.AccountID=data9.user_id)
select AccountID,LoginDate,first_login as regist_time,(case when media_source='' then 'unknown' when first_login<event_time then 'unknown' else media_source END) as media_source,
(case when Campaign='' then 'unknown' when first_login<event_time then 'unknown' else Campaign END) as Campaign,
(case when Adset='' then 'unknown' when first_login<event_time then 'unknown' else Adset END) as Adset,
(case when campaign_type='' then 'unknown' when first_login<event_time then 'unknown' else campaign_type END) as campaign_type from data10
"""
client_ch.insert_dataframe('INSERT INTO da_cdp_funzy.data_retention_mkt VALUES',client_ch.query_dataframe(query=query),settings={
                        'use_numpy': True})
# clickhouse_info1={ "host": "192.168.8.96",
#                 "user": "da_team",
#                 "password": "Ftech@123"}
# client_ch1=Client(host=clickhouse_info1['host'], user=clickhouse_info1['user'],
#                     password=clickhouse_info1['password'], settings={
#                         'use_numpy': True}
# )
# start_date=datetime.date(2022,9,1)
# end_time=datetime.date(2023,4,19)
# delta=timedelta(days=1)
# df=pd.DataFrame()

# while start_date<=end_time:
#     date=start_date.strftime("%Y%m%d")
#     query1=f"""select distinct substring(open_id,4,100) as user_id,'login' as smt from ss_login.t_{date}"""
#     n=client_ch.query_dataframe(query1)
#     start_date+= delta
#     df=pd.concat([df,n])
# df1=df.reset_index(drop=True).drop_duplicates()
# query2="""with temp_data as(
# select user_id,media_source,campaign_type,Adset,event_time,row_number() over (partition by user_id order by event_time asc) as smt from da_cdp_funzy.appsflyer_user_info),
# data1 as(
# select * from temp_data  where user_id<>'' and smt=1
# order by user_id,event_time),
# data2 as(
# select account_id,lpid,fid,row_number() over (partition by account_id order by createTime asc) as smt,createTime from da_cdp_funzy.partner_user_info
# order by account_id),
# regist_data as(
# select account_id,lpid,fid,toDate(createTime) as regist_time from data2 where smt=1
# order by account_id),
# data_test as(
# select * from data1 left join regist_data on data1.user_id=regist_data.fid),
# data_check as(
# select * from data_test where account_id = ''),
# game_fid as(
# select substring(open_id,4,100) as smt from da_cdp_funzy.partner_game_result
# group by smt)
# select user_id,media_source,campaign_type,event_time from data_check left join game_fid on data_check.user_id=game_fid.smt
# where game_fid.smt=''
# """
# data=client_ch1.query_dataframe(query2)
# df_merge=data.merge(df1,how='left',on='user_id')
# print(df_merge[df_merge['smt']=='login'])
# 1000005