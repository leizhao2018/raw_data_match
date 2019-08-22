#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 18 12:41:35 2018
funtion, contact the raw_data_download.py,classify_by_boat.py
check_reformat_data.py and match_tele_raw.py
finally: output the plot and statistics every week

@author: leizhao
"""

import raw_tele_modules as rdm
from datetime import datetime,timedelta
import os
import pandas as pd
import upload_modules as up

def week_start_end(dtime,interval=0):
    '''input a time, 
    if the interval is 0, return this week monday 0:00:00 and next week monday 0:00:00
    if the interval is 1,return  last week monday 0:00:00 and this week monday 0:00:00'''
    delta=dtime-datetime(2003,1,1,0,0)-timedelta(weeks=interval)
    count=int(delta/timedelta(weeks=1))
    start_time=datetime(2003,1,1,0,0)+timedelta(weeks=count)
    end_time=datetime(2003,1,1,0,0)+timedelta(weeks=count+1)   
    return start_time,end_time 
def main():
    realpath=os.path.dirname(os.path.abspath(__file__))
    parameterpath=realpath.replace('py','parameter')
    #HARDCODING
    output_path=realpath.replace('py','result')  #use to save the data 
    picture_save=output_path+'/stats/' #use to save the picture
    emolt='https://www.nefsc.noaa.gov/drifter/emolt.dat' #this is download from https://www.nefsc.noaa.gov/drifter/emolt.dat, 
    telemetry_status=os.path.join(parameterpath,'telemetry_status.csv')
    # below hardcodes is the informations to upload local data to student drifter. 
    subdir=['stats']    
    mremote='/Raw_Data'
    remote_subdir=['stats']
    ###########################
    end_time=datetime.now()
    start_time,end_time=week_start_end(end_time,interval=1)
    if not os.path.exists(picture_save):
        os.makedirs(picture_save)
    print('match telemetered and raw data!')
    #match the telementry data with raw data, calculate the numbers of successful matched and the differnces of two data. finally , use the picture to show the result.
    dict=rdm.match_tele_raw(os.path.join(output_path,'checked'),path_save=os.path.join(picture_save,'statistics'),telemetry_path=emolt,telemetry_status=telemetry_status,\
                        start_time=start_time,end_time=end_time,dpi=500)
    tele_dict=dict['tele_dict']
    raw_dict=dict['raw_dict']
    record_file_df=dict['record_file_df']
    index=tele_dict.keys()
    print('match telemetered and raw data finished!')
    print("start draw map")
    raw_d=pd.DataFrame(data=None,columns=['time','filename','mean_temp','mean_depth','mean_lat','mean_lon'])
    tele_d=pd.DataFrame(data=None,columns=['time','mean_temp','mean_depth','mean_lat','mean_lon'])
    for i in index:
        for j in range(len(record_file_df)): #find the location of data of this boat in record file 
            if i.lower()==record_file_df['Boat'][j].lower():
                break
        if len(raw_dict[i])==0 and len(tele_dict[i])==0:
            continue
        else:
            raw_d=raw_d.append(raw_dict[i])
            tele_d=tele_d.append(tele_dict[i])
            rdm.draw_map(raw_dict[i],tele_dict[i],i,start_time,end_time,picture_save,dpi=300)
            rdm.draw_time_series_plot(raw_dict[i],tele_dict[i],i,start_time,end_time,picture_save,record_file_df.iloc[j],dpi=300)
    raw_d.index=range(len(raw_d))
    tele_d.index=range(len(tele_d))
    rdm.draw_map(raw_d,tele_d,'all_map',start_time,end_time,picture_save,dpi=300)

    for i in range(len(subdir)):
        local_dir=os.path.join(output_path,subdir[i])
        remote_dir=os.path.join(mremote,remote_subdir[i])
        up.sd2drf(local_dir,remote_dir)
#if __name__=='__main__':
#    main()