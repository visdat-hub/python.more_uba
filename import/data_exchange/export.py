import json
import os, sys
import subprocess

class export():
    def __init__(self):
        print("class export")
        
    def json(self, jsondata, path, fname):
        print("--> save as json")
        print("save to : " + str(path) + str(fname))
        
        if not os.path.exists(path):
            os.makedirs(path)
            
        with open(path + fname, 'w') as outfile:
            json.dump(jsondata, outfile)

    def export_grid(self, argDict):
        print("export grid with a specified raster format")
        srcFile = argDict['inFile']
        format = argDict['outFormat']
        if os.path.isfile(srcFile):
            if format == "nc":
                self.sgrd2netcdf(argDict)
        else:
            sys.exit("file not found --> " + srcFile)

    def sgrd2netcdf(self, argDict):
        print("export sgrd to netcdf")
        
        p = subprocess.Popen('/bin/bash', shell=True, stdin=subprocess.PIPE)
        
        saga_string = 'saga_cmd io_gdal "GDAL: Export Raster" ' +\
            '-GRIDS ' + str(argDict['inFile']) + ' ' +\
            '-FILE '  + str(argDict['outFile']) + '.nc ' +\
            '-FORMAT 12 -TYPE ' + str(argDict['dataType']) + ' ' +\
            '-SET_NODATA -NODATA "' + str(argDict['noData']) + '" ' +\
            '-OPTIONS "COMPRESS=DEFLATE ZLEVEL=9 FORMAT=NC4"'
            
        print (saga_string)
        p.communicate(saga_string.encode('utf8'))[0]
        del p

    def netcdf2hdf5(self, argDict):
        print("export netcdf to hdf5")