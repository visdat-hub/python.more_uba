import sys
import subprocess
import math

class gdal_raster_functions():

    def __init__(self):
        print("class gdal_raster_functions")

    #gdal_rasterize [-b band]* [-i] [-at]
    #{[-burn value]* | [-a attribute_name] | [-3d]} [-add]
    #[-l layername]* [-where expression] [-sql select_statement]
    #[-dialect dialect] [-of format] [-a_srs srs_def] [-to NAME=VALUE]*
    #[-co "NAME=VALUE"]* [-a_nodata value] [-init value]*
    #[-te xmin ymin xmax ymax] [-tr xres yres] [-tap] [-ts width height]
    #[-ot {Byte/Int16/UInt16/UInt32/Int32/Float32/Float64/CInt16/CInt32/CFloat32/CFloat64}] [-q]
    #<src_datasource> <dst_filename>
    def rasterize_shape(self, argDict):
        # resamplingMethod is majority
        print("rasterize shapefile")
        sql = -1
        co = ''
        print (argDict)
        # test for decimals
        if argDict['ot'] in ['int8','int16','int32','int64']:
            if int(argDict['decimals']) in [1,2,3,4,5,6]:
                multiplier = math.pow(10,int(argDict['decimals']))
                sql = '"SELECT ' + argDict['a'] + ' * ' + str(multiplier) + ' AS ' + argDict['a'] + ' FROM ' + argDict['src_filename'] + '" '
        if 'co' in argDict:
            co = argDict['co']

        gdal_string = 'gdal_rasterize -a ' + argDict['a'] + ' ' +\
            '-of ' + argDict['of'] + ' ' +\
            co + ' ' +\
            '-a_nodata ' + str(argDict['a_nodata']) + ' ' +\
            argDict['te'] + ' ' +\
            argDict['tr'] + ' ' +\
            '-ot ' + argDict['ot'] + ' '
        if sql != -1:
            gdal_string = gdal_string + '-sql ' + sql
        gdal_string = gdal_string + argDict['src_path'] + argDict['src_filename'] + '.' + argDict['src_format'] + ' ' +\
            argDict['dst_path'] + argDict['dst_filename']
        print (gdal_string)
        p = subprocess.Popen('/bin/bash', shell=True, stdin=subprocess.PIPE)
        p.communicate(gdal_string.encode('utf8'))[0]
        del p

    def resample_grid(self, argDict):
        print("resample a grid")
        print (argDict)
        # command line processes
        if 'subprocess' in sys.modules:
            # resample data
            gdal_string = 'gdalwarp -of ' + argDict['of'] + ' -s_srs EPSG:' + argDict['srcEPSG'] + ' -t_srs EPSG:' + argDict['targetEPSG'] + ' ' +\
                argDict['co'] + ' ' + argDict['resamplingMethod'] + ' ' + argDict['tr'] + ' ' + argDict['te'] + ' -srcnodata ' +\
                argDict['src_nodata'] + ' -dstnodata ' + argDict['dst_nodata'] + ' -overwrite ' +\
                argDict['src_path'] + argDict['src_filename'] + '.' + argDict['src_format'] + ' ' +\
                argDict['dst_path'] + argDict['dst_filename']

            print(gdal_string) 
            #sys.exit()

            p = subprocess.Popen('/bin/bash', shell=True, stdin=subprocess.PIPE)
            p.communicate(gdal_string.encode('utf8'))[0]
            del p
            # set decimals and outFormat to int
            # "outFormat" : [0 : "fileSuffix", 1 : "dtype", 2 : "decimals"]
            if int(argDict['out_format'][2]) in [0,1,2,3,4,5,6]: # maximal 6 decimals
                if argDict['out_format'][1] in ["int8","int16","int32","int64"]:
                    multiplier = math.pow(10,int(argDict['out_format'][2]))
                    gdal_string = 'gdal_calc.py -A ' +\
                        argDict['dst_path'] + argDict['dst_filename'] + ' ' +\
                        '--outfile=' + argDict['dst_path'] + argDict['dst_filename'] + ' --overwrite ' +\
                        '--calc="A*' + str(multiplier) + '" --NoDataValue=' + str(argDict['dst_nodata']) + ' ' +\
                        '--format=' + argDict['of'] + ' ' +\
                        '--type=' + argDict['out_format'][1] + ' ' + argDict['co2']

                    print(gdal_string)
                    p = subprocess.Popen('/bin/bash', shell=True, stdin=subprocess.PIPE)
                    p.communicate(gdal_string.encode('utf8'))[0]
                    del p

    def rasterize_pg(self, argDict):
        print("rasterize a pg datasource (PostgreSQL database table)")
        #print argDict
        co = ''
        ot = ''
        if 'co' in argDict:
            co = argDict['co']
        if 'ot' in argDict:
            ot = '-ot ' + argDict['ot']

        gdal_string = 'gdal_rasterize -a ' + argDict['a'] + ' ' +\
            '-sql "' + argDict['sql'] + '" ' +\
            '-of ' + argDict['of'] + ' ' +\
            co + ' ' +\
            '-a_nodata ' + str(argDict['a_nodata']) + ' ' +\
            argDict['te'] + ' ' +\
            argDict['tr'] + ' ' +\
            ot + ' ' +\
            'PG:"' + argDict['src_pg'] + '" ' +\
            str(argDict['dst_path']) + str(argDict['dst_filename'])
        print (gdal_string)
        sys.exit()
        p = subprocess.Popen('/bin/bash', shell=True, stdin=subprocess.PIPE)
        p.communicate(gdal_string.encode('utf8'))[0]
        del p

    def gdal_translate(self, argDict):
        print("gdal_translate")
        print(argDict)
        gdal_string = 'gdal_translate ' +\
            '-of ' + argDict['of'] + ' ' +\
            argDict['co'] + ' ' +\
            '-r ' + argDict['r'] + ' ' +\
            argDict['srcFile'] + ' ' + argDict['dstFile']
        print (gdal_string)
        p = subprocess.Popen('/bin/bash', shell=True, stdin=subprocess.PIPE)
        p.communicate(gdal_string.encode('utf8'))[0]
        del p

    def setGDAL_createOptions(self, outFormat):
        co = None
        if outFormat == "nc":
            co = '-co "COMPRESS=DEFLATE" -co "ZLEVEL=9" -co "FORMAT=NC4"'
            co2 = '--co "COMPRESS=DEFLATE" --co "ZLEVEL=9" --co "FORMAT=NC4"'
        return co, co2

    def setGDAL_outFormat(self, outFormat):
        of = None
        if outFormat == "nc":
            of = 'netCDF'
        return of

    def setGDAL_targetResolution(self, levelId, gridLevelConfig):
        tr = None
        for item in gridLevelConfig:
            if int(item['levelId']) == int(levelId):
                tr = '-tr ' + str(item['resolution']) + ' ' + str(item['resolution'])
        return tr

    def setGDAL_targetExtent(self, levelId, gridLevelConfig):
        te = None
        for item in gridLevelConfig:
            if int(item['levelId']) == int(levelId):
                te = '-te ' + str(item['minx']) + ' ' + str(item['miny']) + ' ' +\
                    str(item['maxx']) + ' ' + str(item['maxy'])
        return te

    def setGDAL_resamplingMethod(self, resamplingMethod):
        r = None
        if resamplingMethod == "mean":
            r = '-r average'
        if resamplingMethod == "majority":
            r = '-r mode'
        if resamplingMethod == "median":
            r = '-r med'
        if resamplingMethod == "min":
            r = '-r min'
        if resamplingMethod == "max":
            r = '-r max'
        if resamplingMethod == "bilinear":
            r = '-r bilinear'
        if resamplingMethod == "near":
            r = '-r near'
        return r

    def reproject_raster_data(self, argDict):
        print("reproject raster data")
        sys.exit("!! todo")

    #gdal_fillnodata.py [-q] [-md max_distance] [-si smooth_iterations]
    #[-o name=value] [-b band]
    #srcfile [-nomask] [-mask filename] [-of format] [dstfile]
    def fillNoData(self, argDict):
        print("gdal_fillnodata function")
        md = ''

        if 'maxDistance' in argDict:
            if int(argDict['maxDistance']) > 0:
                md = '-md ' + str(argDict['maxDistance'])

        gdal_string = 'gdal_fillnodata.py ' + md + ' ' +\
            argDict['srcFile'] + ' -of ' + argDict['of'] + ' ' + argDict['co'] + ' ' + argDict['dstFile']

        print (gdal_string)
        p = subprocess.Popen('/bin/bash', shell=True, stdin=subprocess.PIPE)
        p.communicate(gdal_string.encode('utf8'))[0]
        del p
