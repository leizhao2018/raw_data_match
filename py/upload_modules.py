#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 14 11:51:21 2019

@author: leizhao
"""
import ftplib
 
import os
import zlconversions as zl



"""def sd2drf(local_dir,remote_dir,filetype='**'):
    '''input local directory and remote directory'''
    
    if local_dir[0]!='/':
        local_dir='/'+local_dir
    if remote_dir[0]!='/':
        remote_dir='/'+remote_dir
    cdflist=zl.list_all_files(local_dir)
    files=[]
    if filetype=='**':
        files=cdflist
    else:    
        for file in cdflist:
            if file.split('.')[1] in filetype:  #determine if the type of the file is in filetype
                files.append(file)
    ftp=ftplib.FTP('66.114.154.52','huanxin','123321')
    drifterlist=list_ftp_allfiles(remote_dir,ftp)
    print(len(drifterlist))
    drflist=[]

    for i in range(len(drifterlist)):
        drflist.append(drifterlist[i].replace(remote_dir,local_dir))
    upflist=list(set(files)-set(drflist))
    print(len(upflist))
    ftp.quit()
    if len(upflist)==0:
        return 0
    for file in upflist:
        ftp=ftplib.FTP('66.114.154.52','huanxin','123321')
        fpath,fname=os.path.split(file)
        remote_dir_file=file.replace(local_dir,remote_dir)
        dir=fpath.replace(local_dir,remote_dir).replace('//','/')
        mkds(dir,ftp)
        ftp_upload(file,remote_dir_file,ftp)
        ftp.quit() 
""" 
def sd2drf(local_dir,remote_dir,filetype='png',keepfolder=False):
    '''function: Upload all files under one folder (including all files under subfolders) to the specified folder 
    input:
        local_dir: local directory
        remote_dir: remote directory,the folder in the student drifters'''
    
    if local_dir[0]!='/':
        local_dir='/'+local_dir
    if remote_dir[0]!='/':
        remote_dir='/'+remote_dir
    cdflist=zl.list_all_files(local_dir)
    files=[]
    if filetype=='**':
        files=cdflist
    else:    
        for file in cdflist:
            if file.split('.')[1] in filetype:
                files.append(file)
    ftp=ftplib.FTP('66.114.154.52','huanxin','123321')
    drifterlist=list_ftp_allfiles(remote_dir,ftp)
    drflist=[]
    if keepfolder:#keep subdirectory
        for i in range(len(drifterlist)):
            drflist.append(drifterlist[i].replace(remote_dir,local_dir))
        upflist=list(set(files)-set(drflist))
        print(len(upflist))
        ftp.quit()
        if len(upflist)==0:
            return 0
        for file in upflist:
            ftp=ftplib.FTP('66.114.154.52','huanxin','123321')
            fpath,fname=os.path.split(file)
            remote_dir_file=file.replace(local_dir,remote_dir)
            dir=fpath.replace(local_dir,remote_dir).replace('//','/')
            mkds(dir,ftp)
            ftp_upload(file,remote_dir_file,ftp)
            ftp.quit() 
    else:  #just upload files,cancel subfolder
        for file in drifterlist:
            fpath,fname=os.path.split(file)
            drflist.append(fname)
     
        upflist=[]
        for file in files:
            fpath,fname=os.path.split(file)
            if fname not in drflist:
                upflist.append(file)
        
        print('the number of upload files:'+str(len(upflist)))
        ftp.quit()
        if len(upflist)==0:
            return 0
        for file in upflist:
            ftp=ftplib.FTP('66.114.154.52','huanxin','123321')
            fpath,fname=os.path.split(file)
            remote_dir_file=file.replace(fpath,remote_dir)
            dir=remote_dir   
            mkds(dir,ftp)
            ftp_upload(file,remote_dir_file,ftp)
            ftp.quit()  

def directory_exists(dir,ftp):
    filelist = []
    ftp.retrlines('LIST',filelist.append)
    for f in filelist:
        if f.split()[-1] == dir and f.upper().startswith('D'):
            return True
    return False

def chdir(dir,ftp): 
    '''Change directories - create if it doesn't exist'''
    if directory_exists(dir,ftp) is False: # (or negate, whatever you prefer for readability)
        ftp.mkd(dir)
        print(dir)
    ftp.cwd(dir)
def ftp_upload(localfile, remotefile,ftp):
  fp = open(localfile, 'rb')
  ftp.storbinary('STOR %s' % os.path.basename(localfile), fp, 1024)
  fp.close()
  print ("after upload " + localfile + " to " + remotefile)
  
def mkds(dir,ftp):
    dir_list=dir.split('/')
    for i in range(len(dir_list)):
        if len(dir_list[i])==0:
            continue
        else:
            chdir(dir_list[i],ftp)        
def list_ftp_allfiles(rootdir,ftp):
    """get all files' path and name in rootdirectory"""
    ftp.cwd('/')
    ftp.cwd(rootdir)
    list = ftp.nlst()
    _files = []
    for i in range(len(list)):
        try:
            path=os.path.join(rootdir,list[i])
            _files.extend(list_ftp_allfiles(path,ftp))
        except ftplib.error_perm:
            path=os.path.join(rootdir,list[i])
            _files.append(path)
    return _files
