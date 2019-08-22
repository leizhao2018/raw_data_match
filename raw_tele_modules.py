
"""
Created on Wed Oct  3 12:39:15 2018
update 2/21 2019
update some the draw_time_series_plot fix the label of y axis
@author: leizhao
"""
import conversions as cv
import ftplib
import glob
import matplotlib.pyplot as plt
import os
import pandas as pd
import numpy as np
import sys
import zlconversions as zl
from datetime import datetime,timedelta
from pylab import mean, std
import conda
conda_file_dir = conda.__file__
conda_dir = conda_file_dir.split('lib')[0]
proj_lib = os.path.join(os.path.join(conda_dir, 'share'), 'proj')
os.environ["PROJ_LIB"] = proj_lib
from mpl_toolkits.basemap import Basemap
import time
try:
    import cPickle as pickle
except ImportError:
    import pickle

#HARDCODES
    
    
from collections import Counter
def dd2dm(lat,lon):
    """
    convert lat, lon from decimal degrees to degrees,minutes
    """
    lat_d = int(abs(lat))                #calculate latitude degrees
    lat_m = (abs(lat) - lat_d) * 60. #calculate latitude minutes
    lon_d = int(abs(lon))
    lon_m = (abs(lon) - lon_d) * 60.
    la=lat_d*100.+lat_m
    lo=lon_d*100.+lon_m
    return la,lo


def gps_compare_JiM(lat,lon,mindistfromharbor): #check to see if the boat is in the harbor    
    # function returns yes if this position is with "mindistfromharbor"  of a dock
    # note: lat and lon need to be in DDMM.M format so if you have decimal degrees you need to first convert using the  dd2dm function
    # we have been using 0.4 in minutes of a degree as our "mindistfromharbor" criteria
    file='/home/jmanning/leizhao/programe/raw_data_match/parameter/harborlist.txt' # has header line lat, lon, harbor
    df=pd.read_csv(file,sep=',')
    indice_lat=[i for i ,v in enumerate(abs(np.array(df['lat'])-lat)<mindistfromharbor) if v]
    indice_lon=[i for i ,v in enumerate(abs(np.array(df['lon'])-lon)<mindistfromharbor) if v]
    harbor_point_list=[i for i, j in zip(indice_lat,indice_lon) if i==j]
    if len(harbor_point_list)>0:
       near_harbor='yes'
    else:
       near_harbor='no'
    return near_harbor #yes or no

def listclean(lista):
    '''Delete consecutive duplicate values ​​and keep only one'''
    a=lista[0]
    llist=[a]
    for i in range(1,len(lista)):
        if a!=lista[i]:
            a=lista[i]
            llist.append(a)
    return llist
            
def weekly_times(name,tstart,tend):
    '''input name and start time and end time, the formay of start and end time is str yyyy-mm-dd
    acording to this information count how often the fisherman fishing'''
    path='https://www.nefsc.noaa.gov/drifter/emolt_ap3_reports.dat'
    df=pd.read_csv(path,sep=',',names=['name','time','lat','lon'])
    for j in df.index:
        if not tstart<=df['time'][j]<=tend:
            df=df.drop(j)
    if len(df)==0:
        return {'yes': 0, 'no': 0}
    dorklist=[]
    for i in df.index:
        if df['name'][i].replace(' ','_')==name.replace(' ','_'):    
            lat1,lon1=dd2dm(lat=df['lat'][i],lon=df['lon'][i])
            dorklist.append(gps_compare_JiM(lat=lat1,lon=lon1,mindistfromharbor=1))
    if len(dorklist)==0:  
        return {'yes': 0, 'no': 0}
    return Counter(listclean(dorklist))

def lasthaul(df,mindepth=5):
    '''function: 
        accroding the depth, choose the last period in the water.
        input: dataframe,
        return dataframe'''
        
    loc_real_data=[]
    allindex=[]
    for i in df.index:
        if df['Depth(m)'][i]<mindepth:
            allindex.append(i)
            
    for i in range(len(allindex)-1):
        if (allindex[i+1]-allindex[i])!=1:
            loc_real_data.append(i)
    if len(loc_real_data)==0:
        return df,1
    else:
        k=len(loc_real_data)
        new_df=df.ix[allindex[loc_real_data[k-1]]:allindex[loc_real_data[k-1]+1],:]
        return new_df,0


def check_reformat_data(indir,outdir,startt,endt,pstatus,rdnf,LSN2='7a',similarity=0.7,mindepth=10):
    """
    input:
        indir:input directory
        LSN2: the first two letters in lowell_sn, for example:Lowell_SN is '7a4c', the LSN2 is '7a', the default value of LSN2 is '7a' 
        rdnf: In this file include the VP_NUM HULL_NUM and VESSEL_NAME 
        check:vessel name,vessel number,serial number, lat,lon
        add VP_NUM
    function:
        fix the format of value, below is the right format
        the header like this:
            Probe Type	Lowell
            Serial Number	c572
            Vessel Number	28
            VP_NUM	310473
            Vessel Name	Dawn_T
            Date Format	YYYY-MM-DD
            Time Format	HH24:MI:SS
            Temperature	C
            Depth	m
        the value like this:
            HEADING	Datet(GMT)	Lat	Lon	Temperature(C)	Depth(m)
            DATA 	2019-03-30 10:37:00	4002.1266	7006.9986 7.71	 0.79
            DATA 	2019-03-30 10:38:30	4002.1289	7006.9934 7.76	 24.2
            DATA 	2019-03-30 10:40:00	4002.1277	7006.9933 7.79	 1.20
        the depth must make sure have some value bigger than mindepth(this is a parameter, the default value is 10)
        if all of depth value is bigger than mindepth, output the logger have some issue
    """
    #Read telemetry status file and raw data name file
    telemetrystatus_df=read_telemetrystatus(pstatus)
    raw_data_name_df=pd.read_csv(rdnf,sep='\t') 
    #produce a dataframe that use to calculate the number of files
    total_df=pd.concat([telemetrystatus_df.loc[:,['Boat']][:],pd.DataFrame(data=[['Total']],columns=['Boat'])],ignore_index=True)
    total_df.insert(1,'file_total',0)
    total_df['Boat']=total_df['Boat'].map(lambda x: x.replace(' ','_'))
    #get all the files under the input folder and screen out the file of '.csv',and put the path+name in the allfile_lists
    allfile_lists=zl.list_all_files(indir)
    file_lists=[]
    for file in allfile_lists:
        fpath,fname=os.path.split(file)  #get the file's path and name
        time_str=fname.split('.')[0].split('_')[2]+' '+fname.split('.')[0].split('_')[3]
        time_gmt=datetime.strptime(time_str,"%Y%m%d %H%M%S")
        if file[len(file)-4:]=='.csv':
            if startt<=time_gmt<=endt:
                file_lists.append(file)
        
    #start check the data and save in the output_dir
    for file in file_lists:
        fpath,fname=os.path.split(file)  #get the file's path and name
        #fix the file name
        fname=file.split('/')[len(file.split('/'))-1]
        if len(fname.split('_')[1])==2:# if the serieal number is only 2 digits make it 4
            new_fname=fname[:3]+LSN2+fname[3:]
        else:
            new_fname=fname
        #read header and data
        try:
            df_head=zl.nrows_len_to(file,2,name=['key','value'])
            df=zl.skip_len_to(file,2) #data
        except KeyboardInterrupt:
            sys.exit()
        except:
            print("worthless file:"+file)
            continue
        vessel_name=fpath.split('/')[-2:-1][0] #get the vessel name
        #check the format of the data
        if len(df.iloc[0])==5: # some files absent the "DATA" in the first column
            df.insert(0,'HEADING','DATA')
        df.columns = ['HEADING','Datet(GMT)','Lat','Lon','Temperature(C)','Depth(m)']  #rename the name of conlum of data
        df['Depth(m)'] = df['Depth(m)'].map(lambda x: '{0:.2f}'.format(float(x)))  #keep two decimal fraction
        datacheck,count=1,0
        for i in range(len(df)):  #the value of count is 0 if the data is test data
            count=count+(float(df['Depth(m)'][i])>mindepth)# keep track of # of depths>mindepth
            if count>5:
                if count==i+1:
                    print('please change the file:'+file+' make sure the logger is work well!' )
                    datacheck=0
                break
        if datacheck==0:
            print(vessel_name+':logger have issue:'+file)
            continue
        if count==0: #if the file is test file,print it
            print ("test file:"+file)
            continue
        df['Temperature(C)'] = df['Temperature(C)'].map(lambda x: '{0:.2f}'.format(float(x))) #keep two decimal fraction
        #keep the lat and lon data format is right,such as 00000.0000w to 0000.0000
        df['Lon'] = df['Lon'].map(lambda x: '{0:.4f}'.format(float(format_lat_lon(x))))
        df['Lat'] = df['Lat'].map(lambda x: '{0:.4f}'.format(float(format_lat_lon(x))))#keep four decimal fraction
        
        #Check if the header file contains all the information, and if it is wrong, fix it.
        for j in range(len(df_head)):#check and fix the vessel number 
            if df_head['key'][j].lower()=='Vessel Number'.lower():
                for i in range(len(telemetrystatus_df)):
                    if telemetrystatus_df['Boat'][i].lower()==vessel_name.lower():
                        df_head['value'][j]=str(telemetrystatus_df['Vessel#'][i])
                        break
                break
        header_file_fixed_key=['Date Format','Time Format','Temperature','Depth'] 
        header_file_fixed_value=['YYYY-MM-DD','HH24:MI:SS','C','m']
        EXIST,loc=0,0
        for fixed_t in header_file_fixed_key:
            for k in range(len(df_head['key'])):
                if fixed_t.lower()==df_head['key'][k].lower():
                    break
                else:
                    EXIST=1
                    count=k+1
            if EXIST==1:
                df_head=pd.concat([df_head[:count],pd.DataFrame(data=[[fixed_t,header_file_fixed_value[loc]]],columns=['key','value'])],ignore_index=True)
            loc+=1 
        for i in range(len(total_df)):#caculate the number of every vessel and boat files
            if total_df['Boat'][i].lower()==vessel_name.lower():
                total_df['file_total'][i]=total_df['file_total'][i]+1
        #if the vessel name and serial number are exist, find the location of them 
        vessel_name_EXIST,S_number_EXIST=0,0
        for k in df_head.index:           
            if df_head['key'][k].lower()=='Vessel Name'.lower():
                vessel_name_EXIST=1
                df_head['value'][k]=vessel_name
            if df_head['key'][k].lower()=='Serial Number'.lower():
                df_head['value'][k]=df_head['value'][k].replace(':','')
                S_number_EXIST=1
        #check and fix the vessel name and serial number 
        if S_number_EXIST==0:
            df_head=pd.concat([df_head[:1],pd.DataFrame(data=[['Serial Number',new_fname.split('_')[1]]],columns=['key','value']),df_head[1:]],ignore_index=True)
        if vessel_name_EXIST==0:#
            df_head=pd.concat([df_head[:2],pd.DataFrame(data=[['Vessel Name',vessel_name]],columns=['key','value']),df_head[2:]],ignore_index=True)
        for i in df_head.index:
            if df_head['key'][i].lower()=='Vessel Number'.lower():
                loc_vp_header=i+1
                break
        for i in raw_data_name_df.index:
            ratio=zl.str_similarity_ratio(vessel_name.lower(),raw_data_name_df['VESSEL_NAME'][i].lower())
            ratio_best=0
            if ratio>similarity:
                if ratio>ratio_best:
                    ratio_best=ratio
                    loc_vp_file=i
        df_head=pd.concat([df_head[:loc_vp_header],pd.DataFrame(data=[['VP_NUM',raw_data_name_df['VP_NUM'][loc_vp_file]]],\
                           columns=['key','value']),df_head[loc_vp_header:]],ignore_index=True)
        #creat the path and name of the new_file and the temperature file  
        output_path=fpath.replace(indir,outdir)
        if not os.path.exists(output_path):   #check the path of the save file is exist,make it if not
            os.makedirs(output_path)
        df_head.to_csv(output_path+'/'+new_fname,index=0,header=0)
        df.to_csv(output_path+'/df_tem.csv',index=0)  #produce the temperature file  
        #add the two file in one file and delet the temperature file
        os.system('cat '+output_path+'/df_tem.csv'+' >> '+output_path+'/'+new_fname)
        os.remove(output_path+'/df_tem.csv')
#    #caculate the total of all files and print save as a file.
    try:
        for i in range(len(total_df)-1):
            total_df['file_total'][len(total_df)-1]=total_df['file_total'][len(total_df)-1]+total_df['file_total'][i]
        total_df.to_csv(outdir+'/items_number.txt',index=0)
    except KeyboardInterrupt:
        sys.exit()
    except:
        print("no valuable file!")

def classify_by_boat(indir,outdir,pstatus):
    """
    indir: input directory, that is the path of read data
    outdir: output directory, that is that path of save data
    pstatus: telemetry_status file
    function:
        accroding the lowell_sn and time to find the file belong to which veseel, and the same vessel produces files put in the same folder.
    notice:this code is suitable for matching data after 2000
    """
    if not os.path.exists(outdir):
        os.makedirs(outdir)
#    if os.listdir(output_dir):
#        print ('please input a empty directory!')
#        sys.exit()
    #read the file of the telementry_status
    df=read_telemetrystatus(pstatus)
    #fix the format of time about logger_change
    for i in df.index:
        if df['logger_change'].isnull()[i]:
            continue
        else:
            date_logger_change=df['logger_change'][i].split(',')   #get the time data of the logger_change
            for j in range(0,len(date_logger_change)):
                if len(date_logger_change[j])>4:     #keep the date have the month and year such as 1/17
                    date_logger_change[j]=zl.transform_date(date_logger_change[j]) #use the transform_date(date) to fix the date
            df['logger_change'][i]=date_logger_change
    #get the path and name of the files
    file_lists=glob.glob(os.path.join(indir,'*.csv'))
    #classify the file        
    for file in file_lists:
        #time conversion, GMT time to local time
        time_str=file.split('/')[len(file.split('/'))-1:][0].split('.')[0].split('_')[2]+' '+file.split('/')[len(file.split('/'))-1:][0].split('.')[0].split('_')[3]
        time_local=zl.gmt_to_eastern(time_str[0:4]+'-'+time_str[4:6]+'-'+time_str[6:8]+' '+time_str[9:11]+':'+time_str[11:13]+':'+time_str[13:15]).strftime("%Y%m%d")
        #match the SN and date
        for i in range(len(df)):
            if df['Lowell-SN'].isnull()[i] or df['logger_change'].isnull()[i]:  #we will enter the next line if SN or date is not exist 
                continue
            else:
                for j in range(len(df['Lowell-SN'][i].split(','))):   
                    fname_len_SN=len(file.split('/')[len(file.split('/'))-1:][0].split('_')[1]) #the length of SN in the file name
                    len_SN=len(df['Lowell-SN'][i].split(',')[j]) #the length of SN in the culumn of the Lowell-SN inthe file of the telemetry_status.csv
                    if df['Lowell-SN'][i].split(',')[j][len_SN-fname_len_SN:]==file.split('/')[len(file.split('/'))-1:][0].split('_')[1]:
                        fpath,fname=os.path.split(file)    #seperate the path and name of the file
                        dstfile=(fpath).replace(indir,outdir+'/'+df['Boat'][i]+'/'+fname.split('_')[2][:6]+'/'+fname) #produce the path+filename of the destination
                        dstfile=dstfile.replace('//','/').replace(' ','_')
                        
                        try:#copy the file to the destination folder
                            if j<len(df['logger_change'][i])-1:
                                if df['logger_change'][i][j]<=time_local<=df['logger_change'][i][j+1]:
                                    zl.copyfile(file,dstfile)  
                            else:
                                if df['logger_change'][i][j]<=time_local:
                                    zl.copyfile(file,dstfile) 
                        except KeyboardInterrupt:
                            sys.exit()
                        except:
                            print(file)
                            print("please check the data of telemetry status!")

def classify_tele_raw_by_boat(input_dir,path_save,telemetry_status,start_time,end_time,telemetry_path='https://www.nefsc.noaa.gov/drifter/emolt.dat',dpi=300):
    """
    the type of start tiem and end time is str :'%Y-%m-%d'
    match the file and telementy.
    we can known how many file send to the satallite and output the figure
    return a dictionary, in this dictionary include raw dictinary and teledictionary and record_file_df
    """
    #read the file of the telementry_status
    telemetrystatus_df=read_telemetrystatus(telemetry_status)
    #st the record file use to write minmum maxmum and average of depth and temperature,the numbers of file, telemetry and successfully matched
    record_file_df=telemetrystatus_df.loc[:,['Boat','Vessel#']].reindex(columns=['Boat','Vessel#','matched_number','file_number','tele_num','max_diff_depth',\
                                      'min_diff_depth','average_diff_depth','max_diff_temp','min_diff_temp','average_diff_temp','sum_diff_depth','sum_diff_temp',\
                                      'min_lat','max_lat','min_lon','max_lon'],fill_value=None)
    #transfer the time format of string to datetime 
    start_time_local=datetime.strptime(start_time,'%Y-%m-%d')
    end_time_local=datetime.strptime(end_time,'%Y-%m-%d')
    allfile_lists=zl.list_all_files(input_dir)
    ######################
    file_lists=[]
    for file in allfile_lists:
        if file[len(file)-4:]=='.csv' and start_time<=file[len(file)-19:len(file)-11]<=end_time:
            file_lists.append(file)
    #download the data of telementry
    tele_df=read_telemetry(telemetry_path)
    #screen out the data of telemetry in interval
    valuable_tele_df=pd.DataFrame(data=None,columns=['vessel_n','esn','time','lon','lat','depth','temp'])#use to save the data during start time and end time
    for i in range(len(tele_df)):
        tele_time=str(tele_df['year'].iloc[i])+'-'+str(tele_df['month'].iloc[i])+'-'+str(tele_df['day'].iloc[i])+' '+\
                                         str(tele_df['Hours'].iloc[i])+':'+str(tele_df['minates'].iloc[i])+':'+'00'
        if zl.local2utc(start_time_local)<=datetime.strptime(tele_time,'%Y-%m-%d %H:%M:%S')<zl.local2utc(end_time_local):
            valuable_tele_df=valuable_tele_df.append(pd.DataFrame(data=[[tele_df['vessel_n'][i],tele_df['esn'][i],tele_time,tele_df['lon'][i],tele_df['lat'][i],\
                                                                         tele_df['depth'][i],tele_df['temp'][i]]],columns=['vessel_n','esn','time','lon','lat','depth','temp']))
    valuable_tele_df.index=range(len(valuable_tele_df))
    #whether the data of file and telemetry is exist
    if len(valuable_tele_df)==0 and len(file_lists)==0:
        print('please check the data website of telementry and the directory of raw_data is exist!')
        #sys.exit()
    elif len(valuable_tele_df)==0:
        print('please check the data website of telementry!')
       # sys.exit()
    elif len(file_lists)==0:
        print('please check the directory raw_data is exist!')
        #sys.exit()
    #match the file
    index=telemetrystatus_df['Boat'] #set the index for dictionary
    raw_dict={}    #the dictinary about raw data, use to write the data about 'time','filename','mean_temp','mean_depth'
    tele_dict={}  #the dictionary about telementry data,use to write the data about'time','mean_temp','mean_depth'
    for i in range(len(index)):  #loop every boat
        raw_dict[index[i]]=pd.DataFrame(data=None,columns=['time','filename','mean_temp','mean_depth','mean_lat','mean_lon'])
        tele_dict[index[i]]=pd.DataFrame(data=None,columns=['time','mean_temp','mean_depth','mean_lat','mean_lon'])
    for file in file_lists: # loop raw files
        fpath,fname=os.path.split(file)  #get the file's path and name
        # now, read header and data of every file  
        header_df=zl.nrows_len_to(file,2,name=['key','value']) #only header 
        data_df=zl.skip_len_to(file,2) #only data
        #caculate the mean temperature and depth of every file
        value_data_df=data_df.ix[(data_df['Depth(m)']>0.85*mean(data_df['Depth(m)']))]  #filter the data
        value_data_df=value_data_df.ix[2:]   #delay several minutes to let temperature sensor record the real bottom temp
        value_data_df=value_data_df.ix[(value_data_df['Temperature(C)']>mean(value_data_df['Temperature(C)'])-3*std(value_data_df['Temperature(C)'])) & \
                   (value_data_df['Temperature(C)']<mean(value_data_df['Temperature(C)'])+3*std(value_data_df['Temperature(C)']))]  #Excluding gross error
        value_data_df.index = range(len(value_data_df))  #reindex
        for i in range(len(value_data_df)):
            value_data_df['Lat'][i],value_data_df['Lon'][i]=cv.dm2dd(value_data_df['Lat'][i],value_data_df['Lon'][i])
        min_lat=min(value_data_df['Lat'].values)
        max_lat=max(value_data_df['Lat'].values)
        min_lon=min(value_data_df['Lon'].values)
        max_lon=max(value_data_df['Lon'].values)
        mean_lat=str(round(mean(value_data_df['Lat'].values),4))
        mean_lon=str(round(mean(value_data_df['Lon'].values),4)) #caculate the mean depth
        mean_temp=str(round(mean(value_data_df['Temperature(C)'][1:len(value_data_df)]),2))
        mean_depth=str(abs(int(round(mean(value_data_df['Depth(m)'].values))))).zfill(3)   #caculate the mean depth
        
        #get the vessel number of every file
        for i in range(len(header_df)):
            if header_df['key'][i].lower()=='vessel number'.lower():
                vessel_number=int(header_df['value'][i])
                break
        #caculate the number of raw files in every vessel,and min,max of lat and lon
        for i in range(len(record_file_df)):
            if record_file_df['Vessel#'][i]==vessel_number:
                if record_file_df['file_number'].isnull()[i]:
                    record_file_df['min_lat'][i]=min_lat
                    record_file_df['max_lat'][i]=max_lat
                    record_file_df['min_lon'][i]=min_lon
                    record_file_df['max_lon'][i]=max_lon
                    record_file_df['file_number'][i]=1
                else:
                    record_file_df['file_number'][i]=int(record_file_df['file_number'][i]+1)
                    if record_file_df['min_lat'][i]>min_lat:
                        record_file_df['min_lat'][i]=min_lat
                    if record_file_df['max_lat'][i]<max_lat:
                        record_file_df['max_lat'][i]=max_lat
                    if record_file_df['min_lon'][i]>min_lon:
                        record_file_df['min_lon'][i]=min_lon
                    if record_file_df['max_lon'][i]<max_lon:
                        record_file_df['max_lon'][i]=max_lon
       
        #match rawdata and telementry data
        time_str=fname.split('.')[0].split('_')[2]+' '+fname.split('.')[0].split('_')[3]
        time_str=time_str[:4]+'-'+time_str[4:6]+'-'+time_str[6:11]+':'+time_str[11:13]+':'+time_str[13:15]
        #write the data of raw file to dict
        for i in range(len(telemetrystatus_df)):
            if telemetrystatus_df['Vessel#'][i]==vessel_number:
                raw_dict[telemetrystatus_df['Boat'][i]]=raw_dict[telemetrystatus_df['Boat'][i]].append(pd.DataFrame(data=[[time_str,\
                                    fname,float(mean_temp),float(mean_depth),float(mean_lat),float(mean_lon)]],\
                                    columns=['time','filename','mean_temp','mean_depth','mean_lat','mean_lon']).iloc[0],ignore_index=True)                             
    #write 'time','mean_temp','mean_depth' of the telementry to tele_dict             
    for i in valuable_tele_df.index:  #valuable_tele_df is the valuable telemetry data during start time and end time 
        for j in range(len(telemetrystatus_df)):
            if int(valuable_tele_df['vessel_n'][i].split('_')[1])==telemetrystatus_df['Vessel#'][j]:
                #count the numbers by boats
                if record_file_df['tele_num'].isnull()[j]:
                    record_file_df['tele_num'][j]=1
                else:
                    record_file_df['tele_num'][j]=record_file_df['tele_num'][j]+1
                if record_file_df['max_lat'].isnull()[j]:
                    record_file_df['min_lat'][j]=valuable_tele_df['lat'][i]
                    record_file_df['max_lat'][j]=valuable_tele_df['lat'][i]
                    record_file_df['min_lon'][j]=valuable_tele_df['lon'][i]
                    record_file_df['max_lon'][j]=valuable_tele_df['lon'][i]
                else:
                    if record_file_df['min_lat'][j]>valuable_tele_df['lat'][i]:
                        record_file_df['min_lat'][j]=valuable_tele_df['lat'][i]
                    if record_file_df['max_lat'][j]<valuable_tele_df['lat'][i]:
                        record_file_df['max_lat'][j]=valuable_tele_df['lat'][i]
                    if record_file_df['min_lon'][j]>valuable_tele_df['lon'][i]:
                        record_file_df['min_lon'][j]=valuable_tele_df['lon'][i]
                    if record_file_df['max_lon'][j]<valuable_tele_df['lon'][i]:
                        record_file_df['max_lon'][j]=valuable_tele_df['lon'][i]
                #write 'time','mean_temp','mean_depth' of the telementry to tele_dict
                tele_dict[telemetrystatus_df['Boat'][j]]=tele_dict[telemetrystatus_df['Boat'][j]].append(pd.DataFrame(data=[[valuable_tele_df['time'][i],\
                         float(valuable_tele_df['temp'][i]),float(valuable_tele_df['depth'][i]),float(valuable_tele_df['lat'][i]),float(valuable_tele_df['lon'][i])]],\
                            columns=['time','mean_temp','mean_depth','mean_lat','mean_lon']).iloc[0],ignore_index=True)
    print("finish the calculate of min_lat and min_lon!")
    for i in range(len(record_file_df)):
        if not record_file_df['matched_number'].isnull()[i]:
            record_file_df['average_diff_depth'][i]=round(record_file_df['sum_diff_depth'][i]/record_file_df['matched_number'][i],4)
            record_file_df['average_diff_temp'][i]=round(record_file_df['sum_diff_temp'][i]/record_file_df['matched_number'][i],4)
        else:
            record_file_df['matched_number'][i]=0
        if record_file_df['tele_num'].isnull()[i]:
            record_file_df['tele_num'][i]=0
        if record_file_df['file_number'].isnull()[i]:
            record_file_df['file_number'][i]=0
    for i in index:#loop every boat,  i represent the name of boat
        raw_dict[i]=raw_dict[i].sort_values(by=['time'])
        raw_dict[i].index=range(len(raw_dict[i]))
    record_file_df=record_file_df.drop(['sum_diff_depth','sum_diff_temp'],axis=1)
    #save the record file
    record_file_df.to_csv(path_save+'/'+start_time+'_'+end_time+' statistics.csv',index=0) 
    dict={}
    dict['raw_dict']=raw_dict
    dict['tele_dict']=tele_dict
    dict['record_file_df']=record_file_df
    return  dict

def download(psave,startt=datetime.strptime('2000-1-1','%Y-%m-%d'),endt=datetime.now()):
    """download the rawdata from web
        startt: start time, datetime.datetime(2000,1,1,0,0)
        endt: end time, datetime.datetime
        we need download files that time within this interval
    """
    ftp=ftplib.FTP('66.114.154.52','huanxin','123321')
    print ('Logging in.')
    ftp.cwd('/Matdata')
    print ('Accessing files')
    filenames = ftp.nlst() # get filenames within the directory OF REMOTE MACHINE
    start_time_utc=zl.local2utc(startt)  #time tranlate from local to UTC
    end_time_utc=zl.local2utc(endt)
    # MAKE THIS A LIST OF FILENAMES THAT WE NEED DOWNLOAD
    download_files=[]
    if not os.path.exists(psave):
        os.makedirs(psave)
    for file in filenames:
        if len(file.split('_'))==4:
            if start_time_utc<=datetime.strptime(file.split('_')[2]+file.split('_')[3].split('.')[0],'%Y%m%d%H%M%S')<end_time_utc:
                download_files.append(file)
    for filename in download_files: # DOWNLOAD FILES   
        local_filename = os.path.join(psave, filename)
        file = open(local_filename, 'wb')
        ftp.retrbinary('RETR '+ filename, file.write)
        file.close()
    ftp.quit() # This is the “polite” way to close a connection
    print ('New files downloaded')


 
def draw_time_series_plot(raw_dict,tele_dict,name,start_time,end_time,path_picture_save,record_file,dpi=300):
    """use to draw the time series plot"""
    
    fig=plt.figure(figsize=(7,4))
    fig.suptitle(name+'\n'+'matches:'+str(int(record_file['matched_number']))+'   telemetered:'+\
                         str(int(record_file['tele_num']))+'   raw_data_uploaded:'+str(int(record_file['file_number'])),fontsize=8, fontweight='bold')
    ax2=fig.add_subplot(212)
    fig.subplots_adjust(left=0.10)
    ax1=fig.add_subplot(211)    
    if len(raw_dict)>0 and len(tele_dict)>0:
        tele_dict['mean_depth']=-1*tele_dict['mean_depth']
        raw_dict['mean_depth']=-1*raw_dict['mean_depth']
        ax1.plot_date(raw_dict['time'],raw_dict['mean_temp'],linestyle='-',alpha=0.5,label='raw_data',marker='d')
        ax1.plot_date(tele_dict['time'],tele_dict['mean_temp'],linestyle='-',alpha=0.5,label='telemetry',marker='^')
        if record_file['matched_number']!=0:
            ax1.set_title('temperature differences of min:'+str(round(record_file['min_diff_temp'],2))+'  max:'+str(round(record_file['max_diff_temp'],2))+\
                          '  average:'+str(round(record_file['average_diff_temp'],3)))
        ax2.plot_date(raw_dict['time'],raw_dict['mean_depth'],linestyle='-',alpha=0.5,label='raw_data',marker='d')
        ax2.plot_date(tele_dict['time'],tele_dict['mean_depth'],linestyle='-',alpha=0.5,label='telemetry',marker='^')
        if record_file['matched_number']!=0:
            ax2.set_title('depth differences of min:'+str(round(record_file['min_diff_depth'],2))+'  max:'+str(round(record_file['max_diff_depth'],2))+\
                          '  average:'+str(round(record_file['average_diff_depth'],3)))
        max_temp=max(np.nanmax(raw_dict['mean_temp'].values),np.nanmax(tele_dict['mean_temp'].values))
        min_temp=min(np.nanmin(raw_dict['mean_temp'].values),np.nanmin(tele_dict['mean_temp'].values))
        max_depth=max(np.nanmax(raw_dict['mean_depth'].values),np.nanmax(tele_dict['mean_depth'].values))
        min_depth=min(np.nanmin(raw_dict['mean_depth'].values),np.nanmin(tele_dict['mean_depth'].values))
    else:
        labels='raw_data'
        markers='d'
        if len(tele_dict)>0:
            raw_dict=tele_dict
            labels='telemetered'
            markers='^'  
        raw_dict['mean_depth']=-1*raw_dict['mean_depth']
        ax2.plot_date(raw_dict['time'],raw_dict['mean_depth'],linestyle='-',alpha=0.5,label=labels,marker=markers)
        ax1.plot_date(raw_dict['time'],raw_dict['mean_temp'],linestyle='-',alpha=0.5,label=labels,marker=markers)    
        max_temp=np.nanmax(raw_dict['mean_temp'].values)
        min_temp=np.nanmin(raw_dict['mean_temp'].values)
        max_depth=np.nanmax(raw_dict['mean_depth'].values)
        min_depth=np.nanmin(raw_dict['mean_depth'].values)
    diff_temp=max_temp-min_temp
    diff_depth=max_depth-min_depth
    if diff_temp==0:
        textend_lim=0.1
    else:
        textend_lim=diff_temp/8.0
    if diff_depth==0:
        dextend_lim=0.1
    else:
        dextend_lim=diff_depth/8.0  
    ax2.legend()
    ax2.set_ylabel('depth(m)',fontsize=8)
    ax2.set_ylim(min_depth-dextend_lim,max_depth+dextend_lim)
    ax2.axes.title.set_size(8)
    ax2.set_xlim(start_time,end_time)
    ax2.tick_params(labelsize=6)
    ax22=ax2.twinx()
    ax22.set_ylabel('depth(feet)',fontsize=8)
    ax22.set_ylim((max_depth+dextend_lim)*3.28084,(min_depth-dextend_lim)*3.28084)
    ax22.invert_yaxis()
    ax22.tick_params(labelsize=6)
    ax1.legend()
    ax1.set_ylabel('Celius',fontsize=8)  
    ax1.set_ylim(min_temp-textend_lim,max_temp+textend_lim)
    ax1.axes.title.set_size(8)
    ax1.set_xlim(start_time,end_time)
    ax1.axes.get_xaxis().set_visible(False)
    ax1.tick_params(labelsize=6)
    ax12=ax1.twinx()
    ax12.set_ylabel('Fahrenheit',fontsize=10)
    ax12.set_ylim((max_temp+textend_lim)*1.8+32,(min_temp-textend_lim)*1.8+32)#conversing the Celius to Fahrenheit
    ax12.invert_yaxis()
    ax12.tick_params(labelsize=6)
    name=name.replace(' ','_')
    path=os.path.join(path_picture_save,name)
    if not os.path.exists(path):
        os.makedirs(path)
    plt.savefig(path+'/'+start_time.strftime('%Y-%m-%d')+'_'+end_time.strftime('%Y-%m-%d')+'.png',dpi=dpi)
 

def draw_map(raw_df,tele_df,name,start_time_local,end_time_local,path_picture_save,dpi=300):
    """
    the type of start_time and end time is datetime.datetime
    use to draw the location of raw file and telemetered produced"""
    #creat map
    #Create a blank canvas  
    fig=plt.figure(figsize=(8,8.5))
    fig.suptitle('F/V '+name,fontsize=24, fontweight='bold')
    if len(raw_df)>0 and len(tele_df)>0:
        start_time=min(raw_df['time'][0],tele_df['time'][0])
        end_time=max(raw_df['time'][len(raw_df)-1],tele_df['time'][len(tele_df)-1])
    elif len(raw_df)>0:
        start_time=raw_df['time'][0]
        end_time=raw_df['time'][len(raw_df)-1]
    else:
        start_time=tele_df['time'][0]
        end_time=tele_df['time'][len(tele_df)-1]
    if type(start_time)!=str:
        start_time=start_time.strftime('%Y-%m-%d')
        end_time=end_time.strftime('%Y-%m-%d')
    ax=fig.add_axes([0.02,0.02,0.9,0.9])
    ax.set_title(start_time+'-'+end_time)
    ax.axes.title.set_size(16)
    
    if len(raw_df)==0:
        max_lat_r,max_lon_r,min_lat_r,min_lon_r=-999,-999,999,999
    else:
        max_lat_r,max_lon_r=max(raw_df['mean_lat']),max(raw_df['mean_lon'])
        min_lat_r,min_lon_r=min(raw_df['mean_lat']),min(raw_df['mean_lon'])
    if len(tele_df)==0:
        max_lat_t,max_lon_t,min_lat_t,min_lon_t=-999,-999,999,999
    else:
        max_lat_t,max_lon_t=max(tele_df['mean_lat']),max(tele_df['mean_lon'])
        min_lat_t,min_lon_t=min(tele_df['mean_lat']),min(tele_df['mean_lon'])
        
    min_lat,max_lat=min(min_lat_t,min_lat_r),max(max_lat_t,max_lat_r)
    min_lon,max_lon=min(min_lon_t,min_lon_r),max(max_lon_t,max_lon_r)
    #keep the max_lon-min_lon>=2
    if (max_lon-min_lon)<=2:
        max_lon=1-(max_lon-min_lon)/2.0+(max_lon+min_lon)/2.0
        min_lon=max_lon-2
    #adjust the max and min,let map have the same width and height 
    if (max_lon-min_lon)>(max_lat-min_lat):
        max_lat=max_lat+((max_lon-min_lon)-(max_lat-min_lat))/2.0
        min_lat=min_lat-((max_lon-min_lon)-(max_lat-min_lat))/2.0
    else:
        max_lon=max_lon+((max_lat-min_lat)-(max_lon-min_lon))/2.0
        min_lon=min_lon-((max_lat-min_lat)-(max_lon-min_lon))/2.0
    #if there only one data in there
    while(not zl.isConnected()):
        time.sleep(120)
    try:
        service = 'Ocean_Basemap'
        xpixels = 5000 
        #Build a map background
        map=Basemap(projection='mill',llcrnrlat=min_lat-0.1,urcrnrlat=max_lat+0.1,llcrnrlon=min_lon-0.1,urcrnrlon=max_lon+0.1,\
                resolution='f',lat_0=(min_lat+max_lat)/2.0,lon_0=(min_lat+max_lat)/2.0,epsg = 4269)
        map.arcgisimage(service=service, xpixels = xpixels, verbose= False)
        if max_lat-min_lat>=3:
            step=int((max_lat-min_lat)/5.0*10)/10.0
        else:
            step=0.5
        
        # draw parallels.
        parallels = np.arange(0.,90.0,step)
        map.drawparallels(parallels,labels=[0,1,0,0],fontsize=10,linewidth=0.0)
        # draw meridians
        meridians = np.arange(180.,360.,step)
        map.drawmeridians(meridians,labels=[0,0,0,1],fontsize=10,linewidth=0.0)
    
        #Draw a scatter plot
        if len(raw_df)>0 and len(raw_df)>0:
            raw_lat,raw_lon=to_list(raw_df['mean_lat'],raw_df['mean_lon'])
            raw_x,raw_y=map(raw_lon,raw_lat)
            ax.plot(raw_x,raw_y,'ro',markersize=6,alpha=0.5,label='raw_data')
            tele_lat,tele_lon=to_list(tele_df['mean_lat'],tele_df['mean_lon'])
            tele_x,tele_y=map(tele_lon,tele_lat)
            ax.plot(tele_x,tele_y,'b*',markersize=6,alpha=0.5,label='telemetry')
            ax.legend()
        else:
            if len(raw_df)>0:
                raw_lat,raw_lon=to_list(raw_df['mean_lat'],raw_df['mean_lon'])
                raw_x,raw_y=map(raw_lon,raw_lat)
                ax.plot(raw_x,raw_y,'ro',markersize=6,alpha=0.5,label='raw_data')
                ax.legend()
            else:
                tele_lat,tele_lon=to_list(tele_df['mean_lat'],tele_df['mean_lon'])
                tele_x,tele_y=map(tele_lon,tele_lat)
                ax.plot(tele_x,tele_y,'b*',markersize=6,alpha=0.5,label='telemetry')
                ax.legend()
        name=name.replace(' ','_')
        if not os.path.exists(path_picture_save+'/'+name+'/'):
            os.makedirs(path_picture_save+'/'+name+'/')
        plt.savefig(path_picture_save+'/'+name+'/'+'location'+'_'+start_time_local.strftime('%Y-%m-%d')+'_'+end_time_local.strftime('%Y-%m-%d')+'.png',dpi=dpi)
        print(name+' finished draw!')
    except KeyboardInterrupt:
        sys.exit()
    except:
        print(name+' need redraw!')
 
def format_lat_lon(data):
    """fix the data of lat and lon"""
    if len(str(data).split('.')[0])>4 or 'A'<=str(data).split('.')[1][len(str(data).split('.')[1])-1:]<='Z':
        data=str(data).split('.')[0][len(str(data).split('.')[0])-4:]+'.'+str(data).split('.')[1][:4]
    return data


def match_tele_raw(input_dir,path_save,telemetry_status,start_time,end_time,telemetry_path='https://www.nefsc.noaa.gov/drifter/emolt.dat',\
                   accept_minutes_diff=20,acceptable_distance_diff=2,dpi=300,Ttdepth=5):
    """
    start time and end time is utc time, and the format is datetime.datetime
    match the file and telementy.
    we can known how many file send to the satallite and output the figure
    """

    #read the file of the telementry_status
    telemetrystatus_df=read_telemetrystatus(telemetry_status)
    #st the record file use to write minmum maxmum and average of depth and temperature,the numbers of file, telemetry and successfully matched
    rcolumns=['Boat','Vessel#','matched_number','file_number','tele_num','fish_times','max_diff_depth',\
              'min_diff_depth','average_diff_depth','max_diff_temp','min_diff_temp',\
              'average_diff_temp','sum_diff_depth','sum_diff_temp','min_lat','max_lat','min_lon','max_lon']
    record_file_df=telemetrystatus_df.loc[:,['Boat','Vessel#']].reindex(columns=rcolumns,fill_value=None)
    #transfer the time format of string to datetime 
#    start_time_local=datetime.strptime(start_time,'%Y-%m-%d')
#    end_time_local=datetime.strptime(end_time,'%Y-%m-%d')
    
#    start_time_utc=zl.local2utc(start_time)
#    end_time_utc=zl.local2utc(end_time)
    
    ######################
    allfile_lists=zl.list_all_files(input_dir)
    file_lists=[]
    for file in allfile_lists:
        if file[len(file)-4:]=='.csv':
            file_lists.append(file)
    #download the data of telementry
    tele_df=read_telemetry(telemetry_path)
    #screen out the data of telemetry in interval
    valuable_tele_df=pd.DataFrame(data=None,columns=['vessel_n','esn','time','lon','lat','depth','temp'])#use to save the data during start time and end time
    for i in tele_df.index:
        tele_time_str=str(tele_df['year'].iloc[i])+'-'+str(tele_df['month'].iloc[i])+'-'+str(tele_df['day'].iloc[i])+' '+str(tele_df['Hours'].iloc[i])+':'+str(tele_df['minates'].iloc[i])+':'+'00'
        tele_time=datetime.strptime(tele_time_str,'%Y-%m-%d %H:%M:%S')
        if start_time<=tele_time<end_time:
            valuable_tele_df=valuable_tele_df.append(pd.DataFrame(data=[[tele_df['vessel_n'][i],tele_df['esn'][i],tele_time,tele_df['lon'][i],tele_df['lat'][i],\
                                                                         tele_df['depth'][i],tele_df['temp'][i]]],columns=['vessel_n','esn','time','lon','lat','depth','temp']))
    valuable_tele_df.index=range(len(valuable_tele_df))
    #whether the data of file and telemetry is exist
#    if len(valuable_tele_df)==0 and len(file_lists)==0:
#        print('please check the data website of telementry and the directory of raw_data is exist!')
#        sys.exit()
#    elif len(valuable_tele_df)==0:
#        print('please check the data website of telementry!')
#        sys.exit()
#    elif len(file_lists)==0:
#        print('please check the directory raw_data is exist!')
#        sys.exit()
    #match the file #set the index for dictionary
    raw_dict={}    #the dictinary about raw data, use to write the data about 'time','filename','mean_temp','mean_depth'
    tele_dict={}  #the dictionary about telementry data,use to write the data about'time','mean_temp','mean_depth'
    for i in telemetrystatus_df['Boat']:  #loop every boat
        raw_dict[i]=pd.DataFrame(data=None,columns=['time','filename','mean_temp','mean_depth','mean_lat','mean_lon'])
        tele_dict[i]=pd.DataFrame(data=None,columns=['time','mean_temp','mean_depth','mean_lat','mean_lon'])
   
    #write 'time','mean_temp','mean_depth' of the telementry to tele_dict            
    for i in valuable_tele_df.index:  #valuable_tele_df is the valuable telemetry data during start time and end time 
        for j in telemetrystatus_df.index:
            if int(valuable_tele_df['vessel_n'][i].split('_')[1])==telemetrystatus_df['Vessel#'][j]:
                if float(valuable_tele_df['depth'][i])<Ttdepth:   #Ttdepth means telemetered minimum standard depth
                    continue
                #count the numbers by boats
                if record_file_df['tele_num'].isnull()[j]:
                    record_file_df['tele_num'][j]=1
                else:
                    record_file_df['tele_num'][j]=record_file_df['tele_num'][j]+1
                if record_file_df['max_lat'].isnull()[j]:
                    record_file_df['min_lat'][j]=valuable_tele_df['lat'][i]
                    record_file_df['max_lat'][j]=valuable_tele_df['lat'][i]
                    record_file_df['min_lon'][j]=valuable_tele_df['lon'][i]
                    record_file_df['max_lon'][j]=valuable_tele_df['lon'][i]
                else:
                    if record_file_df['min_lat'][j]>valuable_tele_df['lat'][i]:
                        record_file_df['min_lat'][j]=valuable_tele_df['lat'][i]
                    if record_file_df['max_lat'][j]<valuable_tele_df['lat'][i]:
                        record_file_df['max_lat'][j]=valuable_tele_df['lat'][i]
                    if record_file_df['min_lon'][j]>valuable_tele_df['lon'][i]:
                        record_file_df['min_lon'][j]=valuable_tele_df['lon'][i]
                    if record_file_df['max_lon'][j]<valuable_tele_df['lon'][i]:
                        record_file_df['max_lon'][j]=valuable_tele_df['lon'][i]
                #write 'time','mean_temp','mean_depth' of the telementry to tele_dict
                tele_dict[telemetrystatus_df['Boat'][j]]=tele_dict[telemetrystatus_df['Boat'][j]].append(pd.DataFrame(data=[[valuable_tele_df['time'][i],\
                         float(valuable_tele_df['temp'][i]),float(valuable_tele_df['depth'][i]),float(valuable_tele_df['lat'][i]),float(valuable_tele_df['lon'][i])]],\
                            columns=['time','mean_temp','mean_depth','mean_lat','mean_lon']).iloc[0],ignore_index=True)
                
    for file in file_lists: # loop raw files
        fpath,fname=os.path.split(file)  #get the file's path and name
        #match rawdata and telementry data
        time_str=fname.split('.')[0].split('_')[2]+' '+fname.split('.')[0].split('_')[3]
        #GMT time to local time of file
        time_gmt=datetime.strptime(time_str,"%Y%m%d %H%M%S")
        if time_gmt<start_time or time_gmt>end_time:
            continue
        
        # now, read header and data of every file  
        header_df=zl.nrows_len_to(file,2,name=['key','value']) #only header 
        data_df=zl.skip_len_to(file,2) #only data
        
        #caculate the mean temperature and depth of every file
        value_data_df=data_df.ix[(data_df['Depth(m)']>0.85*mean(data_df['Depth(m)']))]  #filter the data
        value_data_df=value_data_df.ix[2:]   #delay several minutes to let temperature sensor record the real bottom temp
        value_data_df=value_data_df.ix[(value_data_df['Temperature(C)']>mean(value_data_df['Temperature(C)'])-3*std(value_data_df['Temperature(C)'])) & \
                   (value_data_df['Temperature(C)']<mean(value_data_df['Temperature(C)'])+3*std(value_data_df['Temperature(C)']))]  #Excluding gross error
        value_data_df.index = range(len(value_data_df))  #reindex
        for i in range(len(value_data_df)):
            value_data_df['Lat'][i],value_data_df['Lon'][i]=cv.dm2dd(value_data_df['Lat'][i],value_data_df['Lon'][i])
        min_lat=min(value_data_df['Lat'].values)
        max_lat=max(value_data_df['Lat'].values)
        min_lon=min(value_data_df['Lon'].values)
        max_lon=max(value_data_df['Lon'].values)
        mean_lat=str(round(mean(value_data_df['Lat'].values),4))
        mean_lon=str(round(mean(value_data_df['Lon'].values),4)) #caculate the mean depth
        mean_temp=str(round(mean(value_data_df['Temperature(C)'][1:len(value_data_df)]),2))
        mean_depth=str(abs(int(round(mean(value_data_df['Depth(m)'].values))))).zfill(3)   #caculate the mean depth
        for i in header_df.index:#get the vessel number of every file
            if header_df['key'][i].lower()=='vessel number'.lower():
                vessel_number=int(header_df['value'][i])
                break
        #record number of raw files in every vessel,and min,max of lat and lon
        for i in record_file_df.index:
            if record_file_df['Vessel#'][i]==vessel_number:
                if record_file_df['file_number'].isnull()[i]:
                    record_file_df['min_lat'][i]=min_lat
                    record_file_df['max_lat'][i]=max_lat
                    record_file_df['min_lon'][i]=min_lon
                    record_file_df['max_lon'][i]=max_lon
                    record_file_df['file_number'][i]=1
                else:
                    record_file_df['file_number'][i]=int(record_file_df['file_number'][i]+1)
                    if record_file_df['min_lat'][i]>min_lat:
                        record_file_df['min_lat'][i]=min_lat
                    if record_file_df['max_lat'][i]<max_lat:
                        record_file_df['max_lat'][i]=max_lat
                    if record_file_df['min_lon'][i]>min_lon:
                        record_file_df['min_lon'][i]=min_lon
                    if record_file_df['max_lon'][i]<max_lon:
                        record_file_df['max_lon'][i]=max_lon
                break
        #write the data of raw file to dict
        for i in telemetrystatus_df.index:
            if telemetrystatus_df['Vessel#'][i]==vessel_number:                                                                  #time_local to time_gmt
                raw_dict[telemetrystatus_df['Boat'][i]]=raw_dict[telemetrystatus_df['Boat'][i]].append(pd.DataFrame(data=[[time_gmt,\
                                    fname,float(mean_temp),float(mean_depth),float(mean_lat),float(mean_lon)]],\
                    columns=['time','filename','mean_temp','mean_depth','mean_lat','mean_lon']).iloc[0],ignore_index=True) 
                break
        #calculate the numbers of successful matched, minimum,  maximum and mean values ​​of temperature difference and depth difference,and store this data in record file
        lat,lon=value_data_df['Lat'][len(value_data_df)-1],value_data_df['Lon'][len(value_data_df)-1]
        for i in valuable_tele_df.index:
            if valuable_tele_df['vessel_n'][i].split('_')[1]==str(vessel_number):     
                if abs(valuable_tele_df['time'][i]-time_gmt)<=timedelta(minutes=accept_minutes_diff):  #time match
                    if zl.dist(lat1=lat,lon1=lon,lat2=float(valuable_tele_df['lat'][i]),lon2=float(valuable_tele_df['lon'][i]))<=acceptable_distance_diff:  #distance match               
                        for j in record_file_df.index:
                            if record_file_df['Vessel#'][j]==vessel_number:
                                diff_temp=round((float(mean_temp)-float(valuable_tele_df['temp'][i])),4)
                                diff_depth=round((float(mean_depth)-float(valuable_tele_df['depth'][i])),4)
                                if record_file_df['matched_number'].isnull()[j]:
                                    record_file_df['matched_number'][j]=1
                                    record_file_df['sum_diff_temp'][j]=diff_temp
                                    record_file_df['max_diff_temp'][j]=diff_temp
                                    record_file_df['min_diff_temp'][j]=diff_temp
                                    record_file_df['sum_diff_depth'][j]=diff_depth
                                    record_file_df['max_diff_depth'][j]=diff_depth
                                    record_file_df['min_diff_depth'][j]=diff_depth
                                    
                                else:
                                    record_file_df['matched_number'][j]=int(record_file_df['matched_number'][j]+1)
                                    record_file_df['sum_diff_temp'][j]=record_file_df['sum_diff_temp'][j]+diff_temp
                                    record_file_df['sum_diff_depth'][j]=record_file_df['sum_diff_depth'][j]+diff_depth
                                    if record_file_df['max_diff_temp'][j]<diff_temp:
                                        record_file_df['max_diff_temp'][j]=diff_temp
                                    if record_file_df['min_diff_temp'][j]>diff_temp:
                                        record_file_df['min_diff_temp'][j]=diff_temp
                                    if record_file_df['max_diff_depth'][j]<diff_depth:
                                        record_file_df['max_diff_depth'][j]=diff_depth
                                    if record_file_df['min_diff_depth'][j]>diff_depth:
                                        record_file_df['min_diff_depth'][j]=diff_depth
                                valuable_tele_df.drop(i)
                                break
                                
    for i in record_file_df.index:
        counter=weekly_times(name=record_file_df['Boat'][i],tstart=start_time.strftime('%Y-%m-%d'),tend=end_time.strftime('%Y-%m-%d'))
        try:
            record_file_df['fish_times'][i]=counter['yes']
        except:
            record_file_df['fish_times'][i]=0
        
        if not record_file_df['matched_number'].isnull()[i]:
            record_file_df['average_diff_depth'][i]=round(record_file_df['sum_diff_depth'][i]/record_file_df['matched_number'][i],4)
            record_file_df['average_diff_temp'][i]=round(record_file_df['sum_diff_temp'][i]/record_file_df['matched_number'][i],4)
        else:
            record_file_df['matched_number'][i]=0
        if record_file_df['tele_num'].isnull()[i]:
            record_file_df['tele_num'][i]=0
        if record_file_df['file_number'].isnull()[i]:
            record_file_df['file_number'][i]=0
    
    for i in telemetrystatus_df['Boat']:#loop every boat,
        raw_dict[i]=raw_dict[i].sort_values(by=['time'])
        raw_dict[i].index=range(len(raw_dict[i]))
        
    
    total_tele,total_file,total_matched=0,0,0
    for i in record_file_df.index:
        total_tele+=record_file_df['tele_num'][i]
        total_file+=record_file_df['file_number'][i]
        total_matched+=record_file_df['matched_number'][i]
    record_file_df=pd.concat([record_file_df[:],pd.DataFrame(data=[['Total','',total_matched,total_file,total_tele,'','','','','','','','','','','','','']],\
                              columns=rcolumns)],ignore_index=True)
    record_file_df=record_file_df.drop(['sum_diff_depth','sum_diff_temp'],axis=1)         
    #save the record file
    if not os.path.exists(path_save):
            os.makedirs(path_save)
    record_file_df.to_csv(os.path.join(path_save,start_time.strftime('%Y%m%d')+'_'+end_time.strftime('%Y%m%d')+'_statistics.csv'),index=0) 
    dict={}
    dict['raw_dict']=raw_dict
    dict['tele_dict']=tele_dict
    dict['record_file_df']=record_file_df
    return dict
          

def statistic(input_dir,path_save,telemetry_status,start_time,end_time,telemetry_path='https://www.nefsc.noaa.gov/drifter/emolt.dat',\
              accept_minutes_diff=20,acceptable_distance_diff=2,dpi=300,Ttdepth=5):
    """
    match the file and telementy.
    we can known how many file send to the satallite and output the figure
    """

    #read the file of the telementry_status
    telemetrystatus_df=read_telemetrystatus(telemetry_status)
    #st the record file use to write minmum maxmum and average of depth and temperature,the numbers of file, telemetry and successfully matched
    record_file_df=telemetrystatus_df.loc[:,['Boat','Vessel#']].reindex(columns=['Boat','Vessel#','file_number','tele_num'],fill_value=None)
    allfile_lists=zl.list_all_files(input_dir)
    start_time_utc=zl.local2utc(start_time)
    end_time_utc=zl.local2utc(end_time)
    
    ######################
    file_lists=[]
    for file in allfile_lists:
        if file[len(file)-4:]=='.csv':
            file_lists.append(file)
    #download the data of telementry
    tele_df=read_telemetry(telemetry_path)
    #screen out the data of telemetry in interval
    valuable_tele_df=pd.DataFrame(data=None,columns=['vessel_n','esn','time','lon','lat','depth','temp'])#use to save the data during start time and end time
    for i in tele_df.index:
        tele_time=datetime.strptime(str(tele_df['year'].iloc[i])+'-'+str(tele_df['month'].iloc[i])+'-'+str(tele_df['day'].iloc[i])+' '+\
                                         str(tele_df['Hours'].iloc[i])+':'+str(tele_df['minates'].iloc[i])+':'+'00','%Y-%m-%d %H:%M:%S')
        if start_time_utc<=tele_time<end_time_utc:
            valuable_tele_df=valuable_tele_df.append(pd.DataFrame(data=[[tele_df['vessel_n'][i],tele_df['esn'][i],tele_time,tele_df['lon'][i],\
                                                                         tele_df['lat'][i],tele_df['depth'][i],tele_df['temp'][i]]],\
                                                                            columns=['vessel_n','esn','time','lon','lat','depth','temp']))
    valuable_tele_df.index=range(len(valuable_tele_df))
    #whether the data of file and telemetry is exist

    for file in file_lists: # loop raw files
        fpath,fname=os.path.split(file)  #get the file's path and name
        #match rawdata and telementry data
        time_str=fname.split('.')[0].split('_')[2]+' '+fname.split('.')[0].split('_')[3]
        #GMT time to local time of file
        time_gmt=datetime.strptime(time_str,"%Y%m%d %H%M%S")
        if time_gmt<start_time_utc or time_gmt>end_time_utc:
            continue
        # now, read header and data of every file  
        header_df=zl.nrows_len_to(file,2,name=['key','value']) #only header 
        #get the vessel number of every file
        for i in header_df.index:
            if header_df['key'][i].lower()=='vessel number'.lower():
                vessel_number=int(header_df['value'][i])
                break
        #caculate the number of raw files in every vessel,and min,max of lat and lon
        for i in record_file_df.index:
            if record_file_df['Vessel#'][i]==vessel_number:
                if record_file_df['file_number'].isnull()[i]:
                    record_file_df['file_number'][i]=1
                else:
                    record_file_df['file_number'][i]=int(record_file_df['file_number'][i]+1)
                
    for i in valuable_tele_df.index:  #valuable_tele_df is the valuable telemetry data during start time and end time 
        for j in telemetrystatus_df.index:
            if int(valuable_tele_df['vessel_n'][i].split('_')[1])==telemetrystatus_df['Vessel#'][j]:
                if float(valuable_tele_df['depth'][i])<Ttdepth:   #Ttdepth means telemetered minimum standard depth
                    continue
                #count the numbers by boats
                if record_file_df['tele_num'].isnull()[j]:
                    record_file_df['tele_num'][j]=1
                else:
                    record_file_df['tele_num'][j]=record_file_df['tele_num'][j]+1
    print("finish the calculate of min_lat and min_lon!")
    tele_sum,raw_sum=0,0
    for i in record_file_df.index:
        if record_file_df['tele_num'].isnull()[i]:
            record_file_df['tele_num'][i]=0
        if record_file_df['file_number'].isnull()[i]:
            record_file_df['file_number'][i]=0
        tele_sum+=record_file_df['tele_num'][i]
        raw_sum+=record_file_df['file_number'][i]
    record_file_df=record_file_df.append(pd.DataFrame(data=[['Total',' ',raw_sum,tele_sum]],columns=['Boat','Vessel#','file_number','tele_num']))
   
    #save the record file
    
    record_file_df.to_csv(path_save+'/'+start_time.strftime('%Y%m%d')+'_'+end_time.strftime('%Y%m%d')+'_statistics.csv',index=0) 




def read_telemetrystatus(path_name):
    """read the telementry_status, then return the useful data"""
    data=pd.read_csv(path_name)
    #find the data lines number in the file('telemetry_status.csv')
    for i in range(len(data['vessel (use underscores)'])):
        if data['vessel (use underscores)'].isnull()[i]:
            data_line_number=i
            break
    #read the data about "telemetry_status.csv"
    telemetrystatus_df=pd.read_csv(path_name,nrows=data_line_number)
    as_list=telemetrystatus_df.columns.tolist()
    idex=as_list.index('vessel (use underscores)')
    as_list[idex]='Boat'
    telemetrystatus_df.columns=as_list
    for i in range(len(telemetrystatus_df)):
        telemetrystatus_df['Boat'][i]=telemetrystatus_df['Boat'][i].replace("'","")
        if not telemetrystatus_df['Lowell-SN'].isnull()[i]:
            telemetrystatus_df['Lowell-SN'][i]=telemetrystatus_df['Lowell-SN'][i].replace('，',',')
        if not telemetrystatus_df['logger_change'].isnull()[i]:
            telemetrystatus_df['logger_change'][i]=telemetrystatus_df['logger_change'][i].replace('，',',')
    return telemetrystatus_df

def read_telemetry(path):
    """read the telemetered data and fix a standard format, the return the standard data"""
    while True:
        tele_df=pd.read_csv(path,sep='\s+',names=['vessel_n','esn','month','day','Hours','minates','fracyrday',\
                                          'lon','lat','dum1','dum2','depth','rangedepth','timerange','temp','stdtemp','year'])
        if int(tele_df['year'][-10:-9])==int(datetime.now().year):
            break
        print('read_telemetry redownload data')
        time.sleep(600)
    return tele_df
def time_series_plot(dict,ax1,ax2,start_time,end_time,size,dictlabel='telemetered',double=False,name='Default'):
    """use in draw time plot series function""" 
    if double==True:
        fig=plt.figure(figsize=(size,0.625*size))
        fig.suptitle(name,fontsize=3*size, fontweight='bold')
        size=min(fig.get_size_inches())
        ax1=fig.add_axes([0.1, 0.1, 0.8,0.33])
        ax2=fig.add_axes([0.1, 0.50, 0.8,0.33])
    ax1.plot_date(dict['time'],dict['mean_temp'],linestyle='-',color='b',alpha=0.5,label='OBS',marker='d',markerfacecolor='b')
    ax2.plot_date(dict['time'],dict['mean_depth'],linestyle='-',color='b',alpha=0.5,label='OBS',marker='d',markerfacecolor='b')
    max_t,min_t=np.nanmax(dict['mean_temp'].values),np.nanmin(dict['mean_temp'].values)
    max_d,min_d=np.nanmax(dict['mean_depth'].values),np.nanmin(dict['mean_depth'].values)

    try:
        doppio_dict=dict[['time','doppio_temp','doppio_depth']]
        doppio_dict=doppio_dict.dropna()
        if len(doppio_dict)>0:
            ax1.plot_date(doppio_dict['time'],doppio_dict['doppio_temp'],linestyle='--',color='y',alpha=0.5,label='DOPPIO',marker='d',markerfacecolor='y')
            ax2.plot_date(doppio_dict['time'],doppio_dict['doppio_depth'],linestyle='--',color='y',alpha=0.5,label='DOPPIO',marker='d',markerfacecolor='y')
        doppiomax_t,doppiomin_t=np.nanmax(doppio_dict['doppio_temp'].values),np.nanmin(doppio_dict['doppio_temp'].values)
        doppiomax_d,doppiomin_d=np.nanmax(doppio_dict['doppio_depth'].values),np.nanmin(doppio_dict['doppio_depth'].values) 
    except KeyboardInterrupt:
        sys.exit()
    except:
        doppiomax_t,doppiomin_t,doppiomax_d,doppiomin_d=-9999,9999,-99999,99999
    try: 
        gomofs_dict=dict[['time','gomofs_temp','gomofs_depth']]
        gomofs_dict=gomofs_dict.dropna()   
        if len(gomofs_dict)>0:         
            ax1.plot_date(gomofs_dict['time'],gomofs_dict['gomofs_temp'],linestyle='-.',color='r',alpha=0.5,label='GOMOFS',marker='d',markerfacecolor='r')
            ax2.plot_date(gomofs_dict['time'],gomofs_dict['gomofs_depth'],linestyle='-.',color='r',alpha=0.5,label='GOMOFS',marker='d',markerfacecolor='r')
        gomofsmax_t,gomofsmin_t=np.nanmax(gomofs_dict['gomofs_temp'].values),np.nanmin(gomofs_dict['gomofs_temp'].values)
        gomofsmax_d,gomofsmin_d=np.nanmax(gomofs_dict['gomofs_depth'].values),np.nanmin(gomofs_dict['gomofs_depth'].values) 
    except KeyboardInterrupt:
        sys.exit()
    except:
        gomofsmax_t,gomofsmin_t,gomofsmax_d,gomofsmin_d=-9999,9999,-99999,99999
            
    max_temp=max(max_t,doppiomax_t,gomofsmax_t)
    min_temp=min(min_t,doppiomin_t,gomofsmin_t)
    max_depth=max(max_d,doppiomax_d,gomofsmax_d)
    min_depth=min(min_d,doppiomin_d,gomofsmin_d)
    diff_temp=max_temp-min_temp
    diff_depth=max_depth-min_depth
    if diff_temp==0:
        textend_lim=0.1
    else:
        textend_lim=diff_temp/8.0
    if diff_depth==0:
        dextend_lim=0.1
    else:
        dextend_lim=diff_depth/8.0 
        
    ax1.legend(prop={'size': 0.75*size})
    ax1.set_title(dictlabel)
    ax1.set_ylabel('Celius',fontsize=size)  
    ax1.set_ylim(min_temp-textend_lim,max_temp+textend_lim)
    ax1.axes.title.set_size(2*size)
    if len(dict)==1:
        ax1.set_xlim((dict['time'][0]-timedelta(days=3)),(dict['time'][0]+timedelta(days=4)))
    ax1.axes.get_xaxis().set_visible(False)
    ax1.tick_params(labelsize=0.75*size)
    ax12=ax1.twinx()
    ax12.set_ylabel('Fahrenheit',fontsize=size)
    #conversing the Celius to Fahrenheit
    ax12.set_ylim((max_temp+textend_lim)*1.8+32,(min_temp-textend_lim)*1.8+32)
    ax12.invert_yaxis()
    ax12.tick_params(labelsize=0.75*size)
    #The parts of lable
    ax2.legend(prop={'size': 0.75*size})
    ax2.set_ylabel('depth(m)',fontsize=size)
    ax2.set_ylim(min_depth-dextend_lim,max_depth+dextend_lim)
    if len(dict)==1:
        ax2.set_xlim(dict['time'][0]-timedelta(days=3),dict['time'][0]+timedelta(days=4))
    ax2.tick_params(labelsize=0.75*size)
    ax22=ax2.twinx()
    ax22.set_ylabel('depth(feet)',fontsize=size)
    ax22.set_ylim((max_depth+dextend_lim)*3.28084,(min_depth-dextend_lim)*3.28084)
    ax22.invert_yaxis()
    ax22.tick_params(labelsize=0.75*size)
    
    
def to_list(lat,lon):
    "transfer the format to list"
    x,y=[],[]
    for i in range(len(lat)):
        x.append(lat[i])
        y.append(lon[i])
    return x,y
