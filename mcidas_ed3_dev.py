#!/usr/bin/env python
"""
Created on Mon Mar 16 16:41:25 2015

@author: fwrenn
"""

import re as re
import numpy as np
import matplotlib.pyplot as plt
import os
import glob
import multiprocessing as mp
import time

global count

def read_header(fh,fname):
    """
    Read the Edition3 or Edition4 header.
    """
    header = []
    if 'Ed4' in fname:
        headerlines = 21
    else:
        headerlines = 20
    for i in range(0,headerlines):
        line = fh.readline().decode('utf8').rstrip('\n')
        header.append(line)

    return header

def extractVals(header):
    for line in header:
        if 'num lines' in line:
            scans = line.split()[2]
            
        if 'num elements' in line:
            pixels = line.split()[2]
            
        if 'Lat, Lon,' in line:
            wvlen = re.findall("\d+.\d+",line)
        
        if 'subsat lat' in line:
            subsatlat = line.split()[2]
            
        if 'subsat lon' in line:
            subsatlon = line.split()[2]
            
    return wvlen, int(scans), int(pixels), float(subsatlat), float(subsatlon)

def gen_plt( fname ):
    
    fh = open(fname,'rb')
    # Read the header
    header = read_header(fh,fname)
    # Parse the header to get data-read and plotting information.
    wvlens, scans, pixels, subsatlat, subsatlon = extractVals(header)
    chan = []
    
    #buf = fh.read(2*pixels*scans)
    for iwv in wvlens:
        if int(float(iwv)) == 0:
            chan.append('vis')
        elif int(float(iwv)) == 10 or int(float(iwv)) == 11:
            chan.append('ir')
        elif int(float(iwv)) == 6:
            chan.append('wvapor')
        elif int(float(iwv)) == 3:
            chan.append('nir')
        else:
            pass

    # Instantize the figure, turn off the border
    plt.figure(figsize=(12,12))
    plt.ioff()

    # Must read from the buffer
    # Values are 2-byte integers. 0.01 is scale. Reshape puts the
    # data into the 2D array
    # No need to store lat/lon unless you're gridding the image.
    buf = fh.read(2*pixels*scans)
    buf = fh.read(2*pixels*scans)
    #print ('Creating : ')
    for ichan in chan:

        buf = fh.read(2*pixels*scans)
        rad = np.frombuffer(buf,dtype='>i2').reshape(scans,pixels)*0.01
        rad = np.ma.masked_where(rad < 0.0, rad)
        #rad[rad < 0.0] = np.nan

        # Create plot
        sat = os.path.basename(fname).split('.')[1] 
        plttitle = pltdir+sat+'/'+ichan+'/'+ \
            '.'.join(os.path.basename(fname).split('.')[:-1])+'.'+ichan+'.png'
        #print (plttitle)
        plt.title(os.path.basename(plttitle))

        #cmap = plt.cm.YlGnBu
        cmap = plt.cm.viridis_r
        if ichan == 'vis':
            vmin, vmax = rad.min(), rad.max()
            #cmap = plt.cm.YlGnBu_r
            cmap = plt.cm.viridis
        elif ichan == 'ir':
            vmin, vmax = 190.0, 330.0
        elif ichan == 'wvapor':
            vmin, vmax = 180.0, 270.0
        elif ichan == 'nir':
            vmin, vmax = 190.0, 330.0

        # Simple post image to device
        plt.axis('off')
        plt.tight_layout()
        plt.imshow(rad, cmap=cmap, vmin=vmin, vmax=vmax, interpolation=None)

        plt.savefig(plttitle ,dpi=100)
        plt.clf()

    fh.close()
    plt.close()
    return
    
def process_chunk( fname ):
  
    #for fname in chunk:
    print( fname )
    gen_plt( fname )

#if __name__ == '__main__':

yyyy = '2016'
mm = '06'
ddhh = '*.*'
pltdir = '~/output_directory/'
ed4 = 'n'
archive = '~/input_directory/'

# check for existence of top directory.
if not os.path.exists(pltdir):
    raise Exception('Top directory '+pltdir+' does not exist')
if pltdir[-1] != '/':
    pltdir = pltdir+'/'

# list of satellites in this month
sats = [ 'sat1', 'sat2', 'sat3', 'sat4', 'sat5' ]

flist = []
for isat in sats:
    flist += sorted(glob.glob(archive+'/'+yyyy+'/'+mm+'/MCIDAS.' \
            +isat+'.'+yyyy+'.'+mm+'.'+ddhh+'*'))

    satdir = pltdir+isat+'/'

    if not os.path.exists( satdir ):
        os.makedirs( satdir )
        if ed4.lower() == 'y' or ed4.lower() == 'yes':
            os.makedirs( satdir+'vis' )
            os.makedirs( satdir+'ir' )
            os.makedirs( satdir+'wvapor' )
        else:
            os.makedirs( satdir+'vis' )
            os.makedirs( satdir+'ir' )
            os.makedirs( satdir+'nir' )

pool = mp.Pool(mp.cpu_count())
pool.map( process_chunk, flist )
pool.close()
pool.join()
