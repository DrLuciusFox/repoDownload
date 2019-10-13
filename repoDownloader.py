#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  3 13:57:00 2019

@author: Yashwanth
"""

class config:
    sleep_time = 0
    count = 1
    prev_sleep_time = 0
    pres_sleep_time = 0

from netCDF4 import Dataset
import netCDF4 as nc
import numpy as np
import os
import time

from bs4 import BeautifulSoup
import requests
#import config
from subprocess import DEVNULL, STDOUT, check_call
import sys

global ncfilename
global ncfiledesc


def getBodyTextData(body):
    bodyText = body.text.split('\n')[3::]
    bodyText[:] = (value for value in bodyText if value != '')
    myDict = {}
    for ebt in bodyText:
        eba = ebt.split(' ')
        eba[:] = (value for value in eba if value != '')
        if len(eba)!=4:
            print('Error in bodyText')
            exit(-1)
        lmdt =  eba[1] + eba[2]
        lmdt = lmdt.replace('-', '')
        lmdt = lmdt.replace(':', '')
        nDict = {eba[0]: (lmdt, eba[3])}
        myDict.update(nDict)
    return myDict


def getDwnDirUrls(link, group):
    innrGrpKeys = list(group.groups.keys())
    grpVars0 = list(group.variables)
    grpVars = grpVars0[1::]

    if 'URL' in grpVars0:
        # assuming status exists when URL exists
        # also assuming it didn't fully read the links and directories.
        pass
    else:
        #URL
        group.createDimension('URL', len(link))
        op = group.createVariable('URL', 'S1', 'URL')
        op[:] = nc.stringtochar(np.array([link], 'S'))

        #Status
        group.Status = 0
	
    try:
        thisGrpName = str(group).split('\n')[1].split(':')[0]
    except:
        thisGrpName = 'UNKNOWN'
        
    print(thisGrpName,'and Status is', group.Status)

    if group.Status==0:
        time.sleep(config.sleep_time)
        try:
            stime = time.time()
            source = requests.get(link).text
            etime = time.time()
            config.prev_sleep_time = config.sleep_time
            config.pres_sleep_time =  etime - stime
            config.sleep_time = (config.prev_sleep_time*(config.count-1) + config.pres_sleep_time)/config.count
            config.count = config.count + 1

            if config.count>30:
                config.count = 1
                config.prev_sleep_time = 0

            soup = BeautifulSoup(source, 'lxml')
            body = soup.find('body')
            As = body.find_all('a')
            
            bodyDict = getBodyTextData(body)

            status = 1

            ddCount = 0
            for ea in As:
                ea_href = ea['href']
                ea_text = ea.text
                
                if not (ea_text == 'Parent Directory' or '?' in ea_href):
                    dlink = link + ea_href
                    if '/' in ea_href:
                        innrGrpName = ea_text[0:-1]
                        if innrGrpName in innrGrpKeys:
                            innrGrp = group.groups[innrGrpName]
                        else:
                            innrGrp = group.createGroup(innrGrpName)
                        innerStatus = getDwnDirUrls(dlink, innrGrp)
                        status = status & innerStatus
                    else:
                        if ea_text in grpVars:
                            pass
                        else:
                            group.createDimension(ea_text, len(dlink))
                            op = group.createVariable(ea_text, 'S1', (ea_text))
                            op[:] = nc.stringtochar(np.array([dlink], 'S'))
                            op.DownloadStatus = 0

            group.Status = status

            if status==1:
                print(thisGrpName,': Complete and Status is', group.Status)
            else:
                print(thisGrpName,': Not yet complete and Status is', group.Status)
        except:
            errType = str(sys.exc_info()[0]).split("'")[1]
            print('Stopped due to {} and status is {}'.format(errType, group.Status))
            print('Returning 0')
            
            group.Error = errType
            group.Status = 0
    
    return group.Status










def DownloadUrls(dirName, group):
    innrGrpKeys = list(group.groups.keys())
    grpVars0 = list(group.variables)
    grpVars = grpVars0[1::]

    for indx, var in enumerate(grpVars):
        if group.DownloadStatus==0:
            gd = group[var][:].data
            dnLink = str(b''.join(gd).decode('ascii'))

            try:
                stime = time.time()
                time.sleep(config.sleep_time)
                print('Downloading:', var)
                check_call(['wget', '-P', dirName, '-c', dnLink], stdout=DEVNULL, stderr=STDOUT)
                etime = time.time()
                config.prev_sleep_time = config.sleep_time
                config.pres_sleep_time = etime - stime
                config.sleep_time = (config.prev_sleep_time * (config.count - 1) + config.pres_sleep_time) / config.count
                config.count = config.count + 1
                print('Success')
                op = group.variables[dwnStVars[indx]]
                op[:] = 1
                print('dwnstatus is', group.variables[dwnStVars[indx]].getValue())
            except:
                print('Failure')
                return
        else:
            pass #Already downloaded.

    for eachGroup in innrGrpKeys:
        innrGrp = group.groups[eachGroup]
        innrDirName = dirName + '/' + eachGroup
        DownloadUrls(innrDirName, innrGrp)
