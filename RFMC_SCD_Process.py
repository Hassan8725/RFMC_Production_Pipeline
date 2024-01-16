# -*- coding: utf-8 -*-
"""
Created on Wed Mar  2 09:49:01 2022

@author: Hassan Ahmed◘
"""

''' Section 1 Importing Libraries '''

import numpy as np
import pandas as pd
import datetime as dt
import warnings
import psycopg2
from statsmodels.sandbox.stats.runs import runstest_1samp
from sqlalchemy import create_engine
from sqlalchemy import text
warnings.filterwarnings("ignore")

''' Section 1 End '''

''' Section 2 Defining Functions '''

''' Section 2.1 Loading Data Function '''

def get_data(process_date):
    connection = psycopg2.connect(user="",
                                  password="",
                                  host="",
                                  port="",
                                  database="")
    df=pd.read_sql('''select date(seg_start) as date_col, seg_start as datetime_col, idnt_unique_appel as callid,
                    libl_activite as liblactivite_vaca, num_appel_qualifie as customerid,
                    coalesce(cds_modelling_arpu,0) as  optimization_metric,
                    case when benchmark=1 then 1 else 0 end  as on_off, 
                    case when (coalesce(cds_modelling_arpu,0))> 0 then 1 else 0 end as issale
                    from schema.table_name
                    where libl_activite in ('NECTAR', 'NUM COURTS', 'C2C') 
                    and disp_vdn in (2000436,2000437,2000438,2000440,2000441,2000442,2000432,2000433,
                    2000416,2000417,2000418,2000419,2000420,2000421,2000422)
                    and (num_appel_qualifie ~* '^\d+?$') is true 
                    and num_appel_qualifie not in ('0171025800','0187250000','0187037000', '0000000000')
                    and coalesce(cds_modelling_arpu,0) >= 0
                    and seg_start >='2021-03-02' and seg_start < '{}' 
                                            order by seg_start asc;'''.format(process_date), connection)
    print("Table has been fetched:\n\nShape is: ", df.shape)


    ''' Checking for Incorrect values and changing Datatype '''
    
    df["date_col"]= pd.to_datetime(df["date_col"])

    cnt=0
    for row in df['customerid']:
        if(len(str(row))!=10) or (any(c.isalpha()for c in row)):
            df.loc[cnt,'customerid']=np.nan
        cnt+=1

    df=df[pd.notnull(df['customerid'])]    #we can also use dropna function as well

    df=df[(df['optimization_metric']>=0)]
    
    return df

''' Section 2.1 End '''

''' Section 2.3 Get Unique BTNs for each day Function '''

def get_pday_btns(process_date,process_date_delta):
                         
    connection = psycopg2.connect(user="",
                                  password="",
                                  host="",
                                  port="",
                                  database="")
    
    df=pd.read_sql('''select distinct num_appel_qualifie as btn from schema.table_name
                    where libl_activite in ('NECTAR', 'NUM COURTS', 'C2C') 
                    and disp_vdn in (2000436,2000437,2000438,2000440,2000441,2000442,2000432,2000433,
                    2000416,2000417,2000418,2000419,2000420,2000421,2000422)
                    and (num_appel_qualifie ~* '^\d+?$') is true 
                    and num_appel_qualifie not in ('0171025800','0187250000','0187037000', '0000000000')
                    and coalesce(cds_modelling_arpu,0) >= 0
                    and seg_start >= '{}' and seg_start < '{}';'''.format(process_date,process_date_delta)
                   , connection)
    
    cnt=0
    for row in df['btn']:
        if(len(str(row))!=10) or (any(c.isalpha()for c in row)):
            df.loc[cnt,'btn']=np.nan
        cnt+=1

    df=df[pd.notnull(df['btn'])]    #we can also use dropna function as well
    return df

''' Section 2.2 End '''

''' Section 2.3 Making of RFMC Function '''


def get_rfmc(data):
    
    df=data

    ''' Section 2.3.1 Recency, Frequency and MonetaryValue Part '''
    
    max_date = max(df.date_col) + dt.timedelta(days=1)

    rfmc_start_time=dt.datetime.now()

    RFM_data = df.groupby(['customerid']).agg({'date_col': lambda x: (max_date - x.max()).days,
                                               'callid': 'nunique',
                                               'optimization_metric': 'sum'})

    RFM_data.rename(columns = {'date_col': 'Recency',
                               'callid': 'Frequency',
                               'optimization_metric': 'MonetaryValue'},
                    inplace=True)

    ''' Section 2.3.1 End '''

    ''' Section 2.3.2 Making another dataframe with only optimization_metric>0 to correctly discretize Monetary Value'''

    df_1=df[(df['optimization_metric']>0)]

    RFM_data_1 = df_1.groupby(['customerid']).agg({'date_col': lambda x: (max_date - x.max()).days,
                                               'callid': 'count',
                                               'optimization_metric': 'sum'})

    RFM_data_1.rename(columns = {'date_col': 'Recency',
                               'callid': 'Frequency',
                               'optimization_metric': 'MonetaryValue'},
                    inplace=True)

    Monetary_Qs_df=RFM_data_1.describe()

    Monetary_min=Monetary_Qs_df.loc["min","MonetaryValue"]

    Monetary_25=Monetary_Qs_df.loc["25%","MonetaryValue"]

    Monetary_50=Monetary_Qs_df.loc["50%","MonetaryValue"]

    Monetary_75=Monetary_Qs_df.loc["75%","MonetaryValue"]

    Monetary_max=Monetary_Qs_df.loc["max","MonetaryValue"]

    ''' Section 2.3.2 End '''

    ''' Section 2.3.3 Discretization of Recency, Frequency and MonetaryValue '''

    ## R
    r_labels = range(5,0,-1)

    r_quartiles = pd.qcut(RFM_data['Recency'], 5, labels = r_labels )

    RFM_data = RFM_data.assign(R = r_quartiles.values)

    ## F
    RFM_data['F']= np.where((RFM_data['Frequency']<=1),1,
                            np.where((RFM_data['Frequency']==2),2,
                                   np.where((RFM_data['Frequency']==3),3,
                                           np.where((RFM_data['Frequency']==4),4,5))))

    ## M
    RFM_data['M']=np.where((RFM_data['MonetaryValue']==0),1,
                          np.where((RFM_data['MonetaryValue']>=Monetary_min) & (RFM_data['MonetaryValue']<Monetary_25),2,
                              np.where((RFM_data['MonetaryValue']>=Monetary_25) & (RFM_data['MonetaryValue']<Monetary_50),3,
                                 np.where((RFM_data['MonetaryValue']>=Monetary_50) & (RFM_data['MonetaryValue']<Monetary_75),4
                                                   ,5))))

    ''' Section 2.3.3 End '''

    ''' Section 2.3.4 Clumpiness and its discretized column creation '''

    clumpiness=pd.pivot_table(df, values='callid', index='customerid', columns='date_col', aggfunc='count')

    clumpiness.fillna(0,inplace=True)

    result=[]

    for i in range(len(clumpiness)):
        #print(runstest_1samp(list(xyz.values[i]), correction = False)[1])
        result.append(runstest_1samp(list(clumpiness.values[i]), correction = False)[1])

    clumpiness["result"]=result

    clumpiness.reset_index(drop = False, inplace=True,level=0)

    clumpiness=clumpiness[['customerid','result']]

    clumpiness.columns.name=None

    clumpiness.rename(columns={'result':'Clumpiness'},inplace=True)

    ## C
    clumpiness['C']=clumpiness['Clumpiness'].apply(lambda x: 1 if x<0.5 else 0)

    ''' Section 2.3.4 End '''

    ''' Section 2.3.5 RFM_data and Clumpliness dataframes merging and Loyalty levels calculation '''

    RFMC_data=pd.merge(RFM_data,clumpiness, on='customerid', how='left')

    def join_rfm(x):return (str(x['R']) + str(x['F']) + str(x['M']) + str(x['C']))

    RFMC_data['RFMC_Segment'] = RFMC_data.apply(join_rfm, axis=1)

    RFMC_data['RFMC_Score'] = RFMC_data[['R','F','M','C']].sum(axis=1).astype(int)

    Loyalty_Level = ['Wood','Iron', 'Bronze','Silver','Gold','Platinum']

    Score_cuts = pd.qcut(RFMC_data.RFMC_Score, q = 6, labels = Loyalty_Level)

    RFMC_data['RFMC_Loyalty_Level'] = Score_cuts.values

    RFMC_data.reset_index(drop=True, inplace=True)

    RFMC_data=RFMC_data[['customerid', 'Recency', 'Frequency', 'MonetaryValue', 'Clumpiness','R', 'F', 'M', 'C',
           'RFMC_Segment', 'RFMC_Score', 'RFMC_Loyalty_Level']]

    #RFMC_data.set_index('customerid',inplace=True)
    RFMC_data['pdate'] = process_date

    rfmc_end_time=dt.datetime.now()

    print('Time taken to create complete RFMC: ',rfmc_end_time-rfmc_start_time)
    
    return RFMC_data

''' Section 2.3 End '''

''' Section 2 End '''

''' Section 3 Main Pipeline for Preparing SCD '''

process_start=dt.datetime.now()
rfmc_final=pd.DataFrame()
#delta=(dt.date.today()-dt.date(2022,3,4)).days
delta=2

engine = create_engine("mysql+pymysql://{user}:{password}@ip:port/{db}"
                           .format(user='', password='', 
                                 db = ''))
for i in range(delta,0,-1):
    print(i)
    process_date = str(dt.date.today() - dt.timedelta(i))
    process_date_delta = str(dt.date.today() - dt.timedelta(i-1))
    print(process_date)
    print(process_date_delta)
    df1=get_data(process_date)
    rfmc=get_rfmc(df1)
    btn_df=get_pday_btns(process_date,process_date_delta)
    tt=rfmc.customerid.isin(btn_df["btn"])
    rfmc2=rfmc[tt]
    rfmc3=rfmc2.copy()
    rfmc_final=rfmc_final.append(rfmc2,ignore_index=False)
    print("rfmc_final shape: ", rfmc_final.shape)
    t=text("delete from schema.`rfmc_historicdata_2` where pdate = '{}'".format(process_date))
    engine.execute(t)
    rfmc3.set_index('customerid',inplace=True)
    rfmc3.to_sql('rfmc_historicdata_2', con=engine, if_exists='append')
    print("Rows Inserted in the MySQL Server Table:\n", rfmc3.shape)

print("\nTaken taken for the whole process: ",dt.datetime.now()-process_start)