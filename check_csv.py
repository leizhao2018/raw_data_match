#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 14:32:42 2019

@author: leizhao
"""

import raw_tele_modules as rdm
from datetime import datetime,timedelta
import os
import upload_modules as up
import ftpdownload




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
    raw_data_name_file=os.path.join(parameterpath,'raw_data_name.txt')  #this data conclude the VP_NUM HULL_NUM VESSEL_NAME
    output_path=realpath.replace('py','result')  #use to save the data 
    telemetry_status=os.path.join(parameterpath,'telemetry_status.csv')
    # below hardcodes is the informations to upload local data to student drifter. 
    subdir=['Matdata','checked']
    mremote='/Raw_Data'
    remote_subdir=['Matdata','checked']
    ###########################
    end_time=datetime.now()
    start_time,end_time=week_start_end(end_time,interval=1)
    #download raw data from website
    files=ftpdownload.download(os.path.join(output_path,'Matdata'),ftppath='/Matdata')
    #classify the file by every boat
    rdm.classify_by_boat(indir=os.path.join(output_path,'Matdata'),outdir=os.path.join(output_path,'classified'),pstatus=telemetry_status)
    print('classfy finished!')
    #check the reformat of every file:include header,heading,lat,lon,depth,temperature.
    rdm.check_reformat_data(indir=os.path.join(output_path,'classified'),outdir=os.path.join(output_path,'checked'),startt=start_time,\
                        endt=end_time,pstatus=telemetry_status,rdnf=raw_data_name_file)
    print('check format finished!')
    for i in range(len(subdir)):
        local_dir=os.path.join(output_path,subdir[i])
        remote_dir=os.path.join(mremote,remote_subdir[i])
        up.sd2drf(local_dir,remote_dir,filetype='csv',keepfolder=True)
#if __name__=='__main__':
#    main()