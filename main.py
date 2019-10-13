from netCDF4 import Dataset
import netCDF4 as nc
import numpy as np
import sys

import repoDownloader

def close(fname, closed=0):
    try:
        fname.close()
    except KeyboardInterrupt:
        closed=0
    except (RuntimeError, NameError):
        print('File Closed')
        closed=1
    if closed==0:
        close(fname, closed)


###################################################

try:
	pref_arg = sys.argv[1]
except:
	pref_arg = 'a'

#link = 'http://archive.ubuntu.com/ubuntu/pool/'
link = 'https://download-ib01.fedoraproject.org/pub/'
ncfilename = 'data_centos_epel_v0.nc'
dirName = 'centos_epel/'

try:
    repodata = Dataset(ncfilename, 'r+', format='NETCDF4')
except:
    repodata = Dataset(ncfilename, 'w', format='NETCDF4')

if not (pref_arg=='d' or pref_arg=='D'):
	print('+++++++++++++++++ Getting URLS +++++++++++++++++')
	print('\n')
	repoDownloader.getDwnDirUrls(link, repodata)
else:
	print('++++++++++++ Downloading Repos ++++++++++++')
	print('\n')
	repoDownloader.DownloadUrls(dirName, repodata)

print('Closing....')
close(repodata)
