# -*- coding: utf-8 -*-
"""
Created on Fri Sep 14 15:08:40 2018
update in MARCH 6 add a function find_nd(find the index of nearest distance)
@author: leizhao

directory list in the end

"""
#from __future__ import unicode_literals
#import platform
#import warnings

import re
import pandas as pd
import pytz
import datetime
import os,shutil
import numpy as np
import math
import difflib
import time
import requests

def angle_conversion(a):
    a = np.array(a)
    return a/180*np.pi
    
def copyfile(srcfile,dstfile):
    """copy file from one folder to another folder"""
    if not os.path.isfile(srcfile):
        print ("%s not exist!"%(srcfile))
    else:
        fpath,fname=os.path.split(dstfile) 
        if not os.path.exists(fpath):
            os.makedirs(fpath)
        shutil.copyfile(srcfile,dstfile)    
   
def dist(lat1=0,lon1=0,lat2=0,lon2=0):
    """caculate the distance of two points, return miles
    the format of lat and  lon is 00.00 (dd not dm)"""
    conversion_factor = 0.62137119
    R = 6371.004
    lon1, lat1 = angle_conversion(lon1), angle_conversion(lat1)
    lon2, lat2 = angle_conversion(lon2), angle_conversion(lat2)
    l = R*np.arccos(np.cos(lat1)*np.cos(lat2)*np.cos(lon1-lon2)+\
                        np.sin(lat1)*np.sin(lat2))*conversion_factor
    return l

def ThreeD_dist(lat1=0,lon1=0,lat2=0,lon2=0,h1=0,h2=0):
    """caculate the distance of two points, return meters
    the lat lon format is dd, the unit of h is m"""
    R = 6371.004
    lon1, lat1 = angle_conversion(lon1), angle_conversion(lat1)
    lon2, lat2 = angle_conversion(lon2), angle_conversion(lat2)
    l = R*np.arccos(np.cos(lat1)*np.cos(lat2)*np.cos(lon1-lon2)+\
                        np.sin(lat1)*np.sin(lat2))
    distance=math.sqrt((1000*l)**2+(h1-h2)**2)
    return distance

def find_header_rows(path_name):
    """the lens of header"""
    original_file=pd.read_csv(path_name,nrows=12,names=['0','1','2','3','4','5'])
    for i in range(len(original_file['0'])):
        if original_file['0'][i]=='HEADING':
            header_rows=i
            break 
    return header_rows

def find_nd(target,lat,lon,lats,lons):
    
    """ Bisection method:find the index of nearest distance"""
    row=0
    maxrow=len(lats)-1
    col=len(lats[0])-1
    while col>=0 and row<=maxrow:
        distance=dist(lat1=lats[row][col],lat2=lat,lon1=lons[row][col],lon2=lon)
        if distance<=target:
            break
        elif abs(lats[row][col]-lat)<abs(lons[row][col]-lon):
            col-=1
        else:
            row+=1
    distance=dist(lat1=lats[row][col],lat2=lat,lon1=lons[row][col],lon2=lon)
    row_md,col_md=row,col  #row_md the row of minimum distance
    #avoid row,col out of range in next step
    if row<3:
        row=3
    if col<3:
        col=3
    if row>maxrow-3:
        row=maxrow-3
    if col>len(lats[0])-4:
        col=len(lats[0])-4
    for i in range(row-3,row+3,1):
        for j in range(col-3,col+3,1):
            distance_c=dist(lat1=lats[i][j],lat2=lat,lon1=lons[i][j],lon2=lon)
            if distance_c<=distance:
                distance=distance_c
                row_md,col_md=i,j
    return row_md,col_md



def fitting(point,lat,lon):
    """
    point represent many data include lat lon and z
    format:[[lat,lon,z],[lat1,lon1,z]...]
    """
#represent the value of matrix
    ISum = 0.0
    X1Sum = 0.0
    X2Sum = 0.0
    X1_2Sum = 0.0
    X1X2Sum = 0.0
    X2_2Sum = 0.0
    YSum = 0.0
    X1YSum = 0.0
    X2YSum = 0.0

    for i in range(0,len(point)):
        
        x1i=point[i][0]
        x2i=point[i][1]
        yi=point[i][2]

        ISum = ISum+1
        X1Sum = X1Sum+x1i
        X2Sum = X2Sum+x2i
        X1_2Sum = X1_2Sum+x1i**2
        X1X2Sum = X1X2Sum+x1i*x2i
        X2_2Sum = X2_2Sum+x2i**2
        YSum = YSum+yi
        X1YSum = X1YSum+x1i*yi
        X2YSum = X2YSum+x2i*yi

#  matrix operations
# _mat1 is the mat1 inverse matrix
    m1=[[ISum,X1Sum,X2Sum],[X1Sum,X1_2Sum,X1X2Sum],[X2Sum,X1X2Sum,X2_2Sum]]
    mat1 = np.matrix(m1)
    m2=[[YSum],[X1YSum],[X2YSum]]
    mat2 = np.matrix(m2)
    _mat1 =mat1.getI()
    mat3 = _mat1*mat2

# use list to get the matrix data
    m3=mat3.tolist()
    a0 = m3[0][0]
    a1 = m3[1][0]
    a2 = m3[2][0]
    y = a0+a1*lat+a2*lon

    return y



def fuzzyfinder(user_input, collection):
    suggestions = []
    pattern = '.*?'.join(user_input)    # Converts 'djm' to 'd.*?j.*?m'
    regex = re.compile(pattern)         # Compiles a regex.
    for item in collection:
        match = regex.search(item)      # Checks if the current item matches the regex.
        if match:
            suggestions.append((len(match.group()), match.start(), item))
    return [x for _, _, x in sorted(suggestions)] 

def get_doppio_url(date):
    url='http://tds.marine.rutgers.edu/thredds/dodsC/roms/doppio/2017_da/his/runs/History_RUN_2018-11-12T00:00:00Z'
    return url.replace('2018-11-12',date)


def gmt_to_eastern(times_gmt):
    """GMT time converted to US Eastern Time"""
    eastern = pytz.timezone('US/Eastern')
    gmt = pytz.timezone('Etc/GMT')
    date = datetime.datetime.strptime(str(times_gmt),'%Y-%m-%d %H:%M:%S')
    date_gmt=gmt.localize(date)
    easterndate=date_gmt.astimezone(eastern)
    return easterndate


def  isConnected(address="http://server.arcgisonline.com/ArcGIS"):
    
    "check the internet"
    try:
        html = requests.get(address,timeout=2)
    except:
        return False
    return True

def keep_number(value,integer_num,decimal_digits):
    """keep the lens of value"""    
    #ouput data type is str
    data=str(value)
    if len(data.split('.'))==2:
        integer=data.split('.')[0]
        decimal=data.split('.')[1]
    else:
        integer=data
        decimal=[]
    if integer_num==all:
        integer=integer
    elif len(integer)>integer_num:
        integer=integer[len(integer)-integer_num:]
    elif len(integer)<integer_num:
        for i in range(integer_num-len(integer)):
            integer='0'+integer[:]
    if decimal_digits==all:
        decimal=decimal
    elif len(decimal)>decimal_digits:
        decimal=decimal[:decimal_digits]
    elif len(decimal)<decimal_digits:
        if decimal==[]:
            decimal='0'
            for i in range(decimal_digits-len(decimal)):
                decimal=decimal[:]+'0'
        else:
            for i in range(decimal_digits-len(decimal)):
                decimal=decimal[:]+'0'
    return str(integer+'.'+decimal)
       

def list_all_files(rootdir):
    """get all files' path and name in rootdirectory"""
    _files = []
    list = os.listdir(rootdir) #列出文件夹下所有的目录与文件
    for i in range(0,len(list)):
           path = os.path.join(rootdir,list[i])
           if os.path.isdir(path):
              _files.extend(list_all_files(path))
           if os.path.isfile(path):
              _files.append(path)
    return _files


def list_sd2uv(s,d):
    """aim at the list transform the speed and direction data to the x,y components of the arrow vectors(u,v)"""
    u,v=np.zeros(len(s)),np.zeros(len(s))
    for i in range(len(s)):
        u[i],v[i]=sd2uv(s[i],d[i])
    return u,v
        
    
    
def list_uv2sd(u,v):
    """aim at the list transform the x,y components of the arrow vectors(u,v) to the speed and direction data"""
    s,d=np.zeros(len(u)),np.zeros(len(u))
    for i in range(len(u)):
        s[i],d[i]=uv2sd(u[i],v[i])
    return s,d

def local2utc(local_st):
    """
    the format of time is datetime: eg.:datetime.datetime(2019, 3, 7, 15, 50, 50)
    local time to utc time"""
    time_struct = time.mktime(local_st.timetuple())
    utc_st = datetime.datetime.utcfromtimestamp(time_struct)
    return utc_st


def nrows_len_to(fle,long,name,**kwargs):
    df=pd.read_csv(fle,names=['key','value1','value2','value3','value4','value5'])
    for i in range(len(df)):
        if len(df.iloc[i].dropna())!=long:
            break
    df=df[:i].dropna(axis=1)
    df.columns=name
    return df


def nrows_to(fle,line,name,**kwargs):
    """only read the header"""
    df=pd.read_csv(fle,names=['1','2','3','4','5','6'])
    for i in range(len(df['1'])):
        if df['1'][i]==line:
            break
    df=df[:i].dropna(axis=1)
    df.columns=name
    return df

    
def sd_list_mean(speeds,directions):
    """aim at the list about average of speed and direction"""
    u_total,v_total=0,0
    for a in range(len(speeds)):
        u,v=sd2uv(speeds[a],directions[a])
        u_total=u_total+u
        v_total=v_total+v
    u_mean=u_total/len(speeds)
    v_mean=v_total/len(speeds)
    WS,WD=uv2sd(u_mean,v_mean)
    return WS,WD


     
def sd2uv(s,d):
    """transform the speed and direction data to the x,y components of the arrow vectors(u,v)""" 
    u_t=math.sin(math.radians(d))
    v_t=math.cos(math.radians(d))
    if abs(u_t)==1:
        v=0
        u=float(s)*u_t
    elif abs(v_t)==1:
        u=0
        v=float(s)*v_t
    else:
        u=float(s)*u_t
        v=float(s)*v_t
    return u,v

def skip_len_to(fle,long,**kwargs):
    df=pd.read_csv(fle,names=['key','value1','value2','value3','value4','value5'])
    for i in range(len(df)):
        if len(df.iloc[i].dropna())!=long:
            break
    return pd.read_csv(fle,skiprows=i)


def skip_to(fle, line,**kwargs):
    """only read the data,not read the header"""
    if os.stat(fle).st_size <= 5:
        raise ValueError("File is empty")
    with open(fle) as f:
        pos = 0
        cur_line = f.readline()
        while not cur_line.startswith(line):
            pos = f.tell()
            cur_line = f.readline()
        f.seek(pos)
        return pd.read_csv(f, **kwargs)


def str_similarity_ratio(str1,str2):
    """caculate the rato of similarity in two string """
    return difflib.SequenceMatcher(None, str1, str2).quick_ratio()

def transform_date(date):
    """format the time to 10/26/2018"""
    date=date.replace(' ','')
    if len(date.split('/'))!=3:
        date=date.split('/')[0]+'/'+'01'+'/'+date.split('/')[1]
    if len(date.split('/')[0])==1:
        date='0'+date.split('/')[0]+'/'+date.split('/')[1]+'/'+date.split('/')[2]
    if len(date.split('/')[1])==1:
        date=date.split('/')[0]+'/'+'0'+date.split('/')[1]+'/'+date.split('/')[2]
    if len(date.split('/')[2])==2:
        date=date.split('/')[0]+'/'+date.split('/')[1]+'/'+'20'+date.split('/')[2]
    date_data=date.split('/')[2]+date.split('/')[0]+date.split('/')[1]
    return date_data

def utc2local(utc_st):
    """
    utc_st: the format like this: datetime.datetime(2019, 3, 7, 10, 50, 50)
    UTC time to local time
    """
    now_stamp = time.time()
    local_time = datetime.datetime.fromtimestamp(now_stamp)
    utc_time = datetime.datetime.utcfromtimestamp(now_stamp)
    offset = local_time - utc_time
    local_st = utc_st + offset
    return local_st

def uv2sd(u,v):
    """transform the x,y components of the arrow vectors(u,v) to the speed and direction data"""
#    s=math.sqrt(u**2+v**2)
    s=math.sqrt(np.square(u)+np.square(v))
    if s==0:
        d=0
    else:
        if abs(v/s)==1:
            d=180/np.pi*math.acos(float(v/s))
        elif abs(u/s)==1:
            d=180/np.pi*math.asin(float(u/s))
        else:
            dt=180/np.pi*math.atan(float(u/v))
            if u>0 and v>0:
                d=dt
            elif v<0:
                d=180+dt
            else:
                d=360+dt
    return s,d

    















