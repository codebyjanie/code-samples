#!/usr/bin/env python3

import sys
import os
import glob
import argparse
import csv
import pandas as pd
import numpy as np
import datetime
from datetime import datetime
import dateutil.parser
from dateutil import tz

parser = argparse.ArgumentParser(description="Scripts to calculate retention rate over periods")
parser.add_argument('-p','--posts', help='Single post file from an IMB (export folder))')
parser.add_argument('-f','--folder', help='Folder of multiple post files from IMBs (export folder)')
parser.add_argument('-g','--groupby', help='Filters to partition the data')
parser.add_argument('-t','--timeframe', help='Set the timeframe for the aggregation. month|quarter|half-year|year',required=True)
parser.add_argument('--brand-list', help='CSV file of a subset of brands')
parser.add_argument('--brand-group', help='CSV file of taxonomy of brands')
parser.add_argument('--group-fillna', help='Fill NAs with Competitor - for loreal only',action='store_true')
parser.add_argument('--sapmena', help='For SAPMENA projects only - change retained rate to retention rate per request',action='store_true')
parser.add_argument('--out-all', help='Output the overall retention rates')
parser.add_argument('--out-groupby', help='Output the retention rates for groupby items')
parser.add_argument('--out-plm', help='Output the influencer list with performance metrics for groupby items')
args = parser.parse_args()

if args.posts:
    posts=pd.read_csv(args.posts,low_memory=False)
elif args.folder:
    #import multiple post files from a folder
    path=args.folder
    files = glob.glob(path+'/*.csv')
    dfs = [pd.read_csv(f) for f in files]
    posts = pd.concat(dfs)

if args.brand_list:
    brands=pd.read_csv(args.brand_list, low_memory=False)
    posts=posts.merge(brands, on='group', how='left', indicator=True)
    posts=posts.loc[posts['_merge']=='both']
else:
    pass

if args.brand_group:
    #import brand taxonomy -- optional
    brand_group=pd.read_csv(args.brand_group, low_memory=False)
    #left outer join and add tiers for posts
    posts=posts.merge(brand_group[['brand_id','beauty_group']], 
                                left_on='group', right_on='brand_id', 
                                how='left').drop('brand_id', axis=1)
    posts.drop_duplicates(subset=None, keep="first", inplace=True)
else:
    pass


#include valid posts only (mentions>=1)
posts['mentions']=pd.to_numeric(posts['mentions'],errors='coerce')
posts=posts.loc[posts['mentions']>=1]

#set the timeframe (month, quarter, half-year, year)
timeframe=args.timeframe

#format date column
posts['date'] = pd.to_datetime(posts['date'], errors='coerce') #format='%d%b%Y:%H:%M:%S.%f')
posts['date'] = posts['date'].dt.tz_convert('US/Eastern')
posts['year'] = posts['date'].dt.year.astype(str)

#define the timeframe for the analysis
if timeframe=='month':
    posts['timeframe'] = posts['year']+'-'+posts['date'].dt.quarter.astype(str)
elif timeframe=='quarter':
    posts['timeframe'] = posts['year']+'-Q'+posts['date'].dt.quarter.astype(str)
elif timeframe=='half-year':
    posts['timeframe'] = posts['year']+'-H'+np.ceil(posts['date'].dt.month.astype(int)/6).astype(int).astype(str)
elif timeframe=='year':
    posts['timeframe'] = posts['year']


#fillna for beauty_group with "Competitor" -- if loreal only
if args.group_fillna:
    posts.fillna({'beauty_group':'Competitor'}, inplace=True)


if args.groupby:
#define groupby
    groupby=args.groupby.split(",")

    #define columns for the aggregation
    columns_all_freq=['timeframe','influencer_uid','post_uid']
    columns_groupby_freq=['timeframe']+groupby+['influencer_uid','post_uid']

    columns_all=['timeframe','influencer_uid']
    columns_groupby=['timeframe']+groupby+['influencer_uid']


    #get unique influencers on post level

    #posts_freq: specify number of mentions
    #posts: use 0/1 to indicate if the brands are mentioned
    posts_all_freq=posts[columns_all_freq].drop_duplicates()
    posts_groupby_freq=posts[columns_groupby_freq].drop_duplicates()

    posts_all=posts[columns_all].drop_duplicates()
    posts_groupby=posts[columns_groupby].drop_duplicates()

    #create pivot table for overall (fill all na values with 0)
    table_all_freq=posts_all_freq.pivot_table(index='influencer_uid',columns='timeframe', aggfunc='size', fill_value=0).reset_index()
    table_all=posts_all.pivot_table(index='influencer_uid',columns='timeframe', aggfunc='size', fill_value=0).reset_index()


    #merge columns with delimiter if data needs to be groupped by more than two filters
    if len(groupby)>=2:
        posts_groupby_freq['combined'] = posts_groupby_freq[groupby].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
        posts_groupby['combined'] = posts_groupby[groupby].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
    else:
        posts_groupby_freq['combined'] = posts_groupby_freq[groupby]
        posts_groupby['combined'] = posts_groupby[groupby]

    #create pivot table for groupby (fill all na values with 0)
    table_groupby_freq=posts_groupby_freq.pivot_table(index=['combined','influencer_uid'],columns='timeframe', aggfunc='size', fill_value=0).reset_index()
    table_groupby=posts_groupby.pivot_table(index=['combined','influencer_uid'],columns='timeframe', aggfunc='size', fill_value=0).reset_index()



# RETENTION - ALL
#get collumns of the retention table (columns of timeframe mainly)
agg_all = pd.DataFrame(columns=table_all.columns)

#aggregate mentions for all influencers by period
agg_all.loc['Total'] = table_all.sum(numeric_only=True)

#calculate nb of retained influencers
def calculate_acquired_count(df, df_agg):
    for i in df.columns:
        x = df.columns.get_loc(i)
        if x == 0:
            y = 0
        else:
            y = df.loc[(df.iloc[:,x-1] == 0) & (df.iloc[:,x] == 1), i].count()
        df_agg.loc['Acquired',i]=y
    return df_agg

#calculate nb of retained influencers
def calculate_retained_count(df, df_agg):
    for i in df.columns:
        x = df.columns.get_loc(i)
        if x == 0:
            y = 0
        else:
            y = df.loc[(df.iloc[:,x-1] == 1) & (df.iloc[:,x] == 1), i].count()
        df_agg.loc['Retained',i]=y
    return df_agg

#calculate nb of churned influencers
def calculate_churned_count(df, df_agg):
    for i in df.columns:
        x = df.columns.get_loc(i)
        if x == 0:
            y = 0
        else:
            y = df.loc[(df.iloc[:,x-1] == 1) & (df.iloc[:,x] == 0), i].count()
        df_agg.loc['Churned',i]=y
    return df_agg


#calculate rates
def calculate_acquisition_rate(df_agg):
    for i in df_agg.columns:
        x = df_agg.columns.get_loc(i)
        if x <= 0:
            y = 0
        else:
            try:
                y = df_agg.iloc[1,x]/df_agg.iloc[0,x]
            except:
                y = 0
        df_agg.loc['Acquisition_rate',i]=y
    return df_agg

def calculate_retention_rate(df_agg):
    for i in df_agg.columns:
        x = df_agg.columns.get_loc(i)
        if x <= 0:
            y = 0
        else:
            try:
                y = df_agg.iloc[2,x]/df_agg.iloc[0,x-1]
            except:
                y = 0
        df_agg.loc['Retention_rate',i]=y
    return df_agg

def calculate_churn_rate(df_agg):
    for i in df_agg.columns:
        x = df_agg.columns.get_loc(i)
        if x <= 0:
            y = 0
        else:
            try:
                y = df_agg.iloc[3,x]/df_agg.iloc[0,x-1]
            except:
                y = 0
        df_agg.loc['Churn_rate',i]=y
    return df_agg
        
def calculate_retained_rate(df_agg):
    for i in df_agg.columns:
        x = df_agg.columns.get_loc(i)
        if x <= 0:
            y = 0
        else:
            try:
                y = df_agg.iloc[2,x]/df_agg.iloc[0,x]
            except:
                y = 0
        df_agg.loc['Retained_rate',i]=y
    return df_agg


# RETENTION - GROUPBY
data = table_groupby.copy()
# Calculate totals by brand
total_groupby = pd.DataFrame(columns=data.columns)
total_groupby = data.groupby('combined', as_index=True).sum()
total_groupby.reset_index(inplace=True)
total_groupby.index=['Total']*len(total_groupby)

# calculate nb of influencers by group

def calculate_acquired_count_group(groupby):
    gp = data.groupby(groupby)
    acquired = pd.DataFrame(columns=total_groupby.columns)

    for key, group in gp:
        for i in group.columns:
            x = group.columns.get_loc(i)
            if x == 0:
                y = key
            else:
                y = group.loc[(group.iloc[:,x-1] == 0) & (group.iloc[:,x] == 1), i].count()
            acquired.loc[key,i]=y
        acquired.index.values[:]='Acquired'
        acquired = acquired.drop('influencer_uid', axis=1)
    return acquired

def calculate_retained_count_group(groupby):
    gp = data.groupby(groupby)
    retained = pd.DataFrame(columns=total_groupby.columns)

    for key, group in gp:
        for i in group.columns:
            x = group.columns.get_loc(i)
            if x == 0:
                y = key
            else:
                y = group.loc[(group.iloc[:,x-1] == 1) & (group.iloc[:,x] == 1), i].count()
            retained.loc[key,i]=y
        retained.index.values[:]='Retained'
        retained = retained.drop('influencer_uid', axis=1)
    return retained

def calculate_churned_count_group(groupby):
    gp = data.groupby(groupby)
    churned = pd.DataFrame(columns=total_groupby.columns)

    for key, group in gp:
        for i in group.columns:
            x = group.columns.get_loc(i)
            if x == 0:
                y = key
            else:
                y = group.loc[(group.iloc[:,x-1] == 1) & (group.iloc[:,x] == 0), i].count()
            churned.loc[key,i]=y
        churned.index.values[:]='Churned'
        churned = churned.drop('influencer_uid', axis=1)
    return churned


#calculate rates

def calculate_acquisition_rate_group(groupby):
    gp_rate = agg_count_groupby.groupby(groupby)
    acquisition_rate = pd.DataFrame(columns=agg_count_groupby.columns)
    
    for key, group in gp_rate:
        for i in group.columns:
            x = group.columns.get_loc(i)
            if x == 0:
                y = key
            else:
                try:
                    y = group.iloc[1,x]/group.iloc[0,x]
                except:
                    y = 0
            acquisition_rate.loc[key,i]=y
        acquisition_rate.index.values[:]='Acquisition_rate'
    return acquisition_rate

def calculate_retention_rate_group(groupby):
    gp_rate = agg_count_groupby.groupby(groupby)
    retention_rate = pd.DataFrame(columns=agg_count_groupby.columns)
    
    for key, group in gp_rate:
        for i in group.columns:
            x = group.columns.get_loc(i)
            if x == 0:
                y = key
            else:
                try:
                    y = group.iloc[2,x]/group.iloc[0,x-1]
                except:
                    y = 0
            retention_rate.loc[key,i]=y
        retention_rate.index.values[:]='Retention_rate'
    return retention_rate

def calculate_churn_rate_group(groupby):
    gp_rate = agg_count_groupby.groupby(groupby)
    churn_rate = pd.DataFrame(columns=agg_count_groupby.columns)
    
    for key, group in gp_rate:
        for i in group.columns:
            x = group.columns.get_loc(i)
            if x == 0:
                y = key
            else:
                try:
                    y = group.iloc[3,x]/group.iloc[0,x-1]
                except:
                    y = 0
            churn_rate.loc[key,i]=y
        churn_rate.index.values[:]='Churn_rate'
    return churn_rate

def calculate_retained_rate_group(groupby):
    gp_rate = agg_count_groupby.groupby(groupby)
    retained_rate = pd.DataFrame(columns=agg_count_groupby.columns)
    
    for key, group in gp_rate:
        for i in group.columns:
            x = group.columns.get_loc(i)
            if x == 0:
                y = key
            else:
                try:
                    y = group.iloc[2,x]/group.iloc[0,x]
                except:
                    y = 0
            retained_rate.loc[key,i]=y
        retained_rate.index.values[:]='Retained_rate'
    return retained_rate


if args.out_all:
    #aggregate mentions for acquired, retained and churned influencers
    calculate_acquired_count(table_all,agg_all)
    calculate_retained_count(table_all,agg_all)
    agg_count_overall = calculate_churned_count(table_all,agg_all)

    calculate_acquisition_rate(agg_all)
    calculate_retention_rate(agg_all)
    calculate_churn_rate(agg_all)
    agg_overall = calculate_retained_rate(agg_all)
    agg_overall = agg_overall.drop('influencer_uid',axis=1).reset_index()
else:
    pass

if args.out_groupby:
    #calculate count
    agg_count_groupby = total_groupby.append([calculate_acquired_count_group('combined'),
                                            calculate_retained_count_group('combined'),
                                            calculate_churned_count_group('combined')])

    #calculate rates
    agg_rate_groupby = calculate_acquisition_rate_group('combined').append([calculate_retention_rate_group('combined'),
                                                                            calculate_churn_rate_group('combined'),
                                                                            calculate_retained_rate_group('combined')])
    agg_groupby = agg_count_groupby.append(agg_rate_groupby)


    #get the columns for the agg table
    columns_agg_groupby=groupby+agg_groupby.drop('combined', axis=1).columns.values.tolist()

    #split the combined column
    agg_groupby[groupby] = agg_groupby['combined'].str.split('_', expand=True)
    agg_groupby.drop('combined', axis=1)

    #re-order
    agg_groupby=agg_groupby.loc[:,columns_agg_groupby].reset_index()
else:
    pass



if args.out_plm:
    # Add social performance -- optional
    metrics=['mentions','total_engagements','video_views','reach_for_eng','reach_for_vv']

    #aggregate performance metrics by brand and by influencer with pivot_table
    #beauty_group is optional
    perf=pd.pivot_table(posts, index=['category','group','beauty_group','influencer_uid','influencer_name','tiers','audience_size'], 
                    values=metrics, 
                    aggfunc=np.sum).reset_index()

    #calculate extra metrics
    perf['total_influence']=perf['total_engagements']+perf['video_views']
    perf['eng_rate']=perf['total_engagements']/perf['reach_for_eng']
    perf['eng/vv']=perf['total_engagements']/perf['video_views']
    perf['frequency']=perf['mentions']/1
    perf['total_influence per mention']=perf['total_influence']/perf['mentions']

    #replace all of the inf values with na
    perf = perf.replace([np.inf, -np.inf], np.nan)

    #re-order columns
    columns=['category', 'group', 'beauty_group', 'influencer_uid', 'influencer_name', 'tiers',
            'audience_size', 'mentions', 'frequency','eng_rate','total_engagements', 
            'video_views', 'eng/vv', 'total_influence','total_influence per mention']
    perf=perf.loc[:,columns]

    portfolio = table_groupby.copy()

    #get the columns for the agg table
    columns_portfolio=groupby+table_groupby.drop('combined', axis=1).columns.values.tolist()

    #create portfolio - mentions by influencer and brand, by period
    portfolio[groupby] = portfolio['combined'].str.split('_', expand=True)

    #re-order
    portfolio = portfolio.loc[:,columns_portfolio]

    #merge performance metrics with portfolio
    plm=perf.merge(portfolio, on=groupby+['influencer_uid'], how='left')
    plm=plm.loc[plm['category']!='all']
else:
    pass


if args.sapmena:
    if args.out_all:
        #keep retained_rate and acquisition_rate only
        agg_overall=agg_overall.loc[~agg_overall['index'].isin(['Retention_rate', 'Churn_rate'])]
        #rename retained_rate to retention_rate
        agg_overall['index']=agg_overall['index'].replace({'Retained_rate': 'Retention_rate'})
    else:
        pass

    if args.out_groupby:
        agg_groupby=agg_groupby.loc[~agg_groupby['index'].isin(['Retention_rate', 'Churn_rate'])]
        agg_groupby['index']=agg_groupby['index'].replace({'Retained_rate': 'Retention_rate'})
    else:
        pass
else:
    pass


if args.out_all:
    OUT_ALL=args.out_all
    agg_overall.to_csv(OUT_ALL, index=False, quoting=csv.QUOTE_NONNUMERIC)
else:
    pass


if args.out_groupby:
    OUT_GROUPBY=args.out_groupby
    agg_groupby.to_csv(OUT_GROUPBY, index=False, quoting=csv.QUOTE_NONNUMERIC)
else:
    pass


if args.out_plm:
    OUT_PLM=args.out_plm
    plm.to_csv(OUT_PLM, index=False, quoting=csv.QUOTE_NONNUMERIC)
else:
    pass


print("Done!")