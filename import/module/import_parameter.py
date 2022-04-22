import os, sys
#sys.path.append('/mnt/galfdaten/Programmierung/tools/more_uba/import/data_exchange')
sys.path.append('/mnt/galfdaten/Programmierung/tools/more_uba/import/gis_functions')
#from export import export
from gdal_raster_functions import gdal_raster_functions
from gdal_vector_functions import gdal_vector_functions
import math

class import_parameter():
    
    def __init__(self):
        print("class import_parameter")
        
    def doProcessing(self, gridLevelConfig, preprocessingConfig):
        print("--> start importing parameter")
        #generalDefs =  preprocessingConfig["general"]
        dataSourceDefs = preprocessingConfig["dataSource"]
        #pgDefs = preprocessingConfig["pg_database"]
        rasterizeDefs = preprocessingConfig["rasterize"]
        
        # rasterize different data formats
        for level in rasterizeDefs['targetGridLevel']:
            # rasterize a shapefile
            if dataSourceDefs["inFormat"] == "shp":
                self.rasterize_shape(level, gridLevelConfig, preprocessingConfig)
                
            # resample a *.asc ascii grid (ESRI grid exchange format)
            if dataSourceDefs["inFormat"] in ["asc", "nc", "sdat"]:
                self.resample_grid(level, gridLevelConfig, preprocessingConfig)
                
            # rasterize PostgreSQL datasource
            if dataSourceDefs["inFormat"] == "PG":
                self.rasterize_pg(level, gridLevelConfig, preprocessingConfig)
                
    def rasterize_shape(self, level, gridLevelConfig, preprocessingConfig):
        # burn shape values into a raster
        # resamplingMethod is majority
        print("--> rasterize shapefile")
        r = gdal_raster_functions()
        argDict = {}
        projectDir = preprocessingConfig['general']['projectDir']
        projectName = preprocessingConfig['general']['project']
        scenarioName = str(preprocessingConfig['general']['scenario_name'])
        scenarioId = str(preprocessingConfig['general']['scenario_id'])
        yearsDef = str(preprocessingConfig['general']['yearsDef'])
        inFormat = preprocessingConfig['dataSource']['inFormat']
        srcPath = preprocessingConfig['dataSource']['srcPath']
        srcFileName = preprocessingConfig['dataSource']['srcName']
        attribute2raster = preprocessingConfig['dataSource']['attribute2raster']
        srcEPSG = str(preprocessingConfig['dataSource']['epsg'])
        idParam = str(preprocessingConfig['rasterize']['idParam'])
        targetLevelId = str(level)
        targetEPSG = str(preprocessingConfig['rasterize']['epsg'])
        outFormat = preprocessingConfig['rasterize']['outFormat']
        outNoData = preprocessingConfig['rasterize']['nodata']
        
        targetPath = projectDir + projectName + "/parameters/" + scenarioName + "/" + idParam + "/" + yearsDef + '/'
        targetFileName = idParam + '_' + scenarioId + '_' + targetLevelId + '.' + outFormat[1] + '.' + str(outFormat[2]) + '.' + outFormat[0]
        gdal_co, gdal_co2 = r.setGDAL_createOptions(outFormat[0])
        of = r.setGDAL_outFormat(outFormat[0])
        tr = r.setGDAL_targetResolution(targetLevelId, gridLevelConfig)
        te = r.setGDAL_targetExtent(targetLevelId, gridLevelConfig)
        
        # create path if not exists
        if not os.path.exists(targetPath):
            os.makedirs(targetPath)
            
        argDict['a'] = attribute2raster
        argDict['co'] = gdal_co
        argDict['of'] = of
        argDict['ot'] = outFormat[1]
        argDict['decimals'] = outFormat[2]
        argDict['a_nodata'] = outNoData
        argDict['te'] = te
        argDict['tr'] = tr
        argDict['a_srs'] = targetEPSG
        argDict['src_path'] = srcPath
        argDict['src_filename'] = srcFileName
        argDict['src_format'] = inFormat
        argDict['src_srs'] = srcEPSG
        argDict['dst_path'] = targetPath
        argDict['dst_filename'] = targetFileName
        argDict['dst_srs'] = targetEPSG
        # call gdal_raster_functions class
        if srcEPSG != targetEPSG:
            print("reprojection of data source necessary")
            print("source epsg: " + srcEPSG)
            print("target epsg: " + targetEPSG)
            argDict['src_filename'] = self.reproject_gisdata(argDict)
        r.rasterize_shape(argDict)
        
    def resample_grid(self, level, gridLevelConfig, preprocessingConfig):
        print("--> resample a grid")
        argDict = {}
        r = gdal_raster_functions()
        
        projectDir = preprocessingConfig['general']['projectDir']
        project = preprocessingConfig['general']['project']
        scenarioId = preprocessingConfig['general']['scenario_id']
        scenarioName = preprocessingConfig['general']['scenario_name']
        yearsDef = preprocessingConfig['general']['yearsDef']
        srcPath =  preprocessingConfig['dataSource']['srcPath']
        srcFileName = preprocessingConfig['dataSource']['srcName']
        inFormat = preprocessingConfig['dataSource']['inFormat']
        srcEPSG = str(preprocessingConfig['dataSource']['epsg'])
        srcnodata = preprocessingConfig['dataSource']['nodata']
        idParam = str(preprocessingConfig['rasterize']['idParam'])
        #targetLevelId = str(level)
        resamplingMethod = preprocessingConfig['rasterize']['resamplingMethod']
        targetEPSG = str(preprocessingConfig['rasterize']['epsg'])
        outFormat = preprocessingConfig['rasterize']['outFormat']
        dstnodata = preprocessingConfig['rasterize']['nodata']
        
        # check inFile
        if os.path.isfile(srcPath + srcFileName + '.' + inFormat):
        	print("source file: " + srcPath + srcFileName + '.' + inFormat )
        else:
        	sys.exit('source file missing: ' + srcPath + srcFileName + '.' + inFormat)
            
        print("resampling method: " + resamplingMethod)
        # loop throug resampling levels and resample grids by means of gdalwarp
        outFile = idParam + '_' + str(scenarioId) + '_' + str(level) + '.' + str(outFormat[1]) + '.' + str(outFormat[2]) + '.' + str(outFormat[0])
        #outFile = idParam + '_' + str(scenarioId) + '_' + str(level) + '.' + str(outFormat[1]) + '.2.' + str(outFormat[0])
        targetPath = projectDir + project + "/parameters/" + scenarioName + "/" + idParam + "/" + yearsDef + '/'
        gdal_of = r.setGDAL_outFormat(outFormat[0])
        gdal_co, gdal_co2 = r.setGDAL_createOptions(outFormat[0])
        gdal_r =  r.setGDAL_resamplingMethod(resamplingMethod)
        gdal_tr = r.setGDAL_targetResolution(level, gridLevelConfig)
        gdal_te = r.setGDAL_targetExtent(level, gridLevelConfig)
        gdal_srcnodata = str(srcnodata)
        gdal_dstnodata = str(dstnodata)
        #gdal_ot = '-ot ' + outFormat[1]
        
        # create path if not exists
        if not os.path.exists(targetPath):
            os.makedirs(targetPath)
            
        print("grid level: " + str(level))
        print('target: ' + outFile)
        
        argDict['co'] = gdal_co
        argDict['co2'] = gdal_co2
        argDict['of'] = gdal_of
        argDict['ot'] = outFormat[1]
        argDict['dst_nodata'] = gdal_dstnodata
        argDict['src_nodata'] = gdal_srcnodata
        argDict['te'] = gdal_te
        argDict['tr'] = gdal_tr
        argDict['a_srs'] = targetEPSG
        argDict['src_path'] = srcPath
        argDict['src_filename'] = srcFileName
        argDict['src_format'] = inFormat
        argDict['out_format'] = outFormat
        argDict['src_srs'] = srcEPSG
        argDict['dst_path'] = targetPath
        argDict['dst_filename'] = outFile
        argDict['dst_srs'] = targetEPSG
        argDict['resamplingMethod'] = gdal_r
        
        
        argDict['srcEPSG'] = srcEPSG
        argDict['targetEPSG'] = targetEPSG
        
        # call gdal_raster_functions class
        #if srcEPSG != targetEPSG:
        #    print("reprojection of data source necessary")
        #    print("source epsg: " + srcEPSG)
        #    print("target epsg: " + targetEPSG)
        #    argDict['src_filename'] = self.reproject_gisdata(argDict)
            
        r.resample_grid(argDict)
        
    def rasterize_pg(self, level, gridLevelConfig, preprocessingConfig):
        print("--> rasterize vector data from a PostgreSQL database table")
        argDict = []
        r = gdal_raster_functions()
        argDict = {}
        projectDir = preprocessingConfig['general']['projectDir']
        projectName = preprocessingConfig['general']['project']
        scenarioName = str(preprocessingConfig['general']['scenario_name'])
        scenarioId = str(preprocessingConfig['general']['scenario_id'])
        yearsDef = str(preprocessingConfig['general']['yearsDef'])
        inFormat = preprocessingConfig['dataSource']['inFormat']
        #srcPath = preprocessingConfig['dataSource']['srcPath']
        srcEPSG = str(preprocessingConfig['dataSource']['epsg'])
        attribute2raster = str(preprocessingConfig['dataSource']['attribute2raster'])
        dbschema = str(preprocessingConfig['dataSource']['srcPath'])
        dbtable = str(preprocessingConfig['dataSource']['srcName'])
        idParam = str(preprocessingConfig['rasterize']['idParam'])
        targetLevelId = str(level)
        targetEPSG = str(preprocessingConfig['rasterize']['epsg'])
        outFormat = preprocessingConfig['rasterize']['outFormat']
        outNoData = preprocessingConfig['rasterize']['nodata']
        
        targetPath = projectDir + projectName + "/parameters/" + scenarioName + "/" + idParam + "/" + yearsDef + '/'
        targetFileName = idParam + '_' + scenarioId + '_' + targetLevelId + '.' + outFormat[1] + '.' + str(outFormat[2]) + '.' + outFormat[0]
        gdal_co, gdal_co2 = r.setGDAL_createOptions(outFormat[0])
        of = r.setGDAL_outFormat(outFormat[0])
        tr = r.setGDAL_targetResolution(targetLevelId, gridLevelConfig)
        te = r.setGDAL_targetExtent(targetLevelId, gridLevelConfig)
        
        multiplier = 1.0
        if int(outFormat[2]) in [0,1,2,3,4,5,6]: # maximal 6 decimals
                if outFormat[1] in ["int8","int16","int32","int64"]:
                        multiplier = math.pow(10,int(outFormat[2]))
                        
        sql_string = "SELECT (" + attribute2raster + " * " + str(multiplier) + ")::numeric as id, " +\
            "the_geom FROM " + dbschema + "." + dbtable
            
        dbhost = preprocessingConfig["pg_database"]["db_host"]
        dbname = preprocessingConfig["pg_database"]["db_name"]
        dbuser = preprocessingConfig["pg_database"]["db_user"]
        dbport = preprocessingConfig["pg_database"]["db_port"]
        dbpwd  = preprocessingConfig["pg_database"]["db_password"]
        src_pg = "host="+dbhost+" dbname="+dbname+" user="+dbuser+" password="+dbpwd+" port="+str(dbport)
        
        # create path if not exists
        if not os.path.exists(targetPath):
            os.makedirs(targetPath)
            
        argDict['a'] = 'id'
        argDict['co'] = gdal_co
        argDict['of'] = of
        argDict['ot'] = outFormat[1]
        argDict['a_nodata'] = outNoData
        argDict['te'] = te
        argDict['tr'] = tr
        argDict['a_srs'] = targetEPSG
        argDict['sql'] = sql_string
        argDict['src_pg'] = src_pg
        argDict['src_format'] = inFormat
        argDict['src_srs'] = srcEPSG
        argDict['dst_path'] = targetPath
        argDict['dst_filename'] = targetFileName
        argDict['dst_srs'] = targetEPSG
        # call gdal_raster_functions class
        if srcEPSG != targetEPSG:
            print("reprojection of data source necessary")
            print("source epsg: " + srcEPSG)
            print("target epsg: " + targetEPSG)
            argDict['src_filename'] = self.reproject_gisdata(argDict)
        r.rasterize_pg(argDict)
        
    def reproject_gisdata(self, config):
        print("--> reproject gis data")
        argDict = {}
        dst_filename = config['src_filename'] + '_epsg' + config['a_srs']
        argDict["src_file"] = config['src_path'] + config['src_filename'] + '.' + config['src_format']
        argDict["dst_file"] = config['src_path'] + dst_filename + '.' + config['src_format']
        argDict["s_srs"] = config['src_srs']
        argDict["t_srs"] = config['dst_srs']
        
        print('-->:', argDict)
        
        
        v = gdal_vector_functions()
        if config['src_format'] == 'shp':
            v.reproject_vector_data(argDict)
        if config['src_format'] in ["asc", "nc", "sgrd"]:
            v.reproject_raster_data(argDict)
        return dst_filename