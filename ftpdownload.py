#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 11:17:41 2019

@author: leizhao
"""
import os
import ftplib

def csv_files(file_list):
    """pick up all .csv files"""
    _files=[]
    for i in range(len(file_list)):
        if file_list[i].split('.')[1]=='csv':
            _files.append(file_list[i])
    return _files

def download(localpath,ftppath):
    '''download the raw data from the student drifter'''
    ftp=ftplib.FTP('66.114.154.52','huanxin','123321')
    print ('Logging in.')
    print ('Accessing files')
    allfilelisthis=csv_files(list_all_files(localpath)) #get all filename and file path exist
    list_all_ftpfiles(ftp,rootdir=ftppath,localpath=localpath,local_list=allfilelisthis)  #download the new raw data there is not exist in local directory 
    allfilelistnew=csv_files(list_all_files(localpath))  #get all filename and file path exist after update
    files=list(set(allfilelistnew)-set(allfilelisthis)) #get the list of filename and filepath that updated
    ftp.quit() # This is the “polite” way to close a connection
    print ('New files downloaded')
    return files

def list_all_files(rootdir):
    """pick up all files' path and name in rootdirectory"""
    _files = []
    list = os.listdir(rootdir) #List all the directories and files under the folder
    for i in range(0,len(list)):
           path = os.path.join(rootdir,list[i])
           if os.path.isdir(path):
              _files.extend(list_all_files(path))
           if os.path.isfile(path):
              _files.append(path)
    return _files

def list_all_ftpfiles(ftp,rootdir,localpath,local_list):
    """get all files' path and name in rootdirectory this is for student drifter"""

    ftp.cwd(rootdir)
    if not os.path.exists(localpath):
        os.makedirs(localpath)
    filelist = ftp.nlst() #List all the directories and files under the folder
    for i in range(0,len(filelist)):
        filepath = os.path.join(localpath,filelist[i])
        if len(filelist[i].split('.'))!=1:
            if filepath in local_list:
                continue
            else:
                file = open(filepath, 'wb')
                ftp.retrbinary('RETR '+ filelist[i], file.write)
                file.close()
        else:
            ftp.cwd('/')
            rootdirnew=os.path.join(rootdir,filelist[i])
            localpathnew=os.path.join(localpath,filelist[i])
            list_all_ftpfiles(ftp=ftp,rootdir=rootdirnew,localpath=localpathnew,local_list=local_list)