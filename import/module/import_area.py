import os, sys
#sys.path.append('/mnt/galfdaten/Programmierung/tools/more_uba/python/data_exchange')
sys.path.append('/mnt/galfdaten/Programmierung/tools/more_uba/import/gis_functions')
sys.path.append('/mnt/galfdaten/Programmierung/tools/more_uba/import/pg')
#from export import export
from gdal_raster_functions import gdal_raster_functions
from pg import db_connector
#from osgeo import gdal,ogr
from osgeo import ogr

class import_area():
    def __init__(self):
        print("class import_area")

    def doProcessing(self, gridLevelConfig, preprocessingConfig):
        print("--> start importing area")
        
        # rasterize different data formats
        # rasterize a shapefile
        if preprocessingConfig["dataSource"]["inFormat"] == "shp":
            # create area in db
            idArea = self.create_area_in_db(preprocessingConfig)
            # create geometries in database
            self.create_area_data_in_db(preprocessingConfig, idArea)
            
        # rasterize pg datasource
        for level in preprocessingConfig["rasterize"]["targetGridLevel"]:
            self.rasterize_pg(level, gridLevelConfig, preprocessingConfig, idArea)


    def create_area_in_db(self, preprocessingConfig):
            # create area in pg_database
            # return an unique area identifier
            print("--> create area as a vector gis data in a PostgreSQL database")
            idArea = None
            create_options = preprocessingConfig["create_area_in_db"]
            db_config = preprocessingConfig["pg_database"]
            table = "spatial.area"
            cols = 'area, description, de, eng, lnk, idgistyp, idlayer, idlayer_level, idhover, statistic, aktiv, srid, type, zindex, ' +\
                        'sequence_stats_filter, sequence_stats_select, sequence_map_filter, sequence_map_select'
                        
            valuePlaceholder = '%(area)s, %(description)s, %(de)s, %(eng)s, %(lnk)s, %(idgistyp)s, %(idlayer)s, %(idlayer_level)s, %(idhover)s, %(statistic)s, %(aktiv)s, %(srid)s, %(type)s, %(zindex)s, ' +\
                        '%(sequence_stats_filter)s, %(sequence_stats_select)s, %(sequence_map_filter)s, %(sequence_map_select)s'
                        
            returnParameter = "idarea"
            
            values = {}
            for key in create_options["columns"]:
                print (key,  create_options["columns"][key])
                if create_options["columns"][key] == "":
                    values[key] = None
                else:
                    values[key] = create_options["columns"][key]
                    
            pg = db_connector()
            pg.dbConfig(db_config)
            pg.dbConnect()
            idArea = pg.tblInsert( table, cols,  valuePlaceholder,  values,  returnParameter  )
            pg.dbClose()

            return idArea

    # insert area data into spatial_slave.area_data_0
    def create_area_data_in_db(self,  preprocessingConfig, idArea):
        """create area data in spatial_slave.area_data_0"""
        """keine Umlaute im Namen der Spalte in der dbf"""
        
        print("--> create area data in table spatial_slave.area_data_0 for idarea... " + str(idArea))
        configuration = preprocessingConfig
        db_config = configuration["pg_database"]
        idarea_data = None
        
        # data source
        filename = configuration["dataSource"]["srcName"] + '.shp'
        path = configuration["dataSource"]["srcPath"]
        attribute2raster = configuration["dataSource"]["attribute2raster"]
        attributes2import = configuration["dataSource"]["attributes2import"]
        srid = configuration["dataSource"]["epsg"]
        # postgis target
        # data table
        table_data = "spatial.area_data"
        cols_data = "idarea, area_data, description_text, description_int, de, eng"
        valuePlaceholder_data = "%(idarea)s, %(area_data)s, %(description_text)s, %(description_int)s, %(de)s, %(eng)s"
        returnParameter_data = "idarea_data"
        # geom table
        table_geom = "spatial.area_geom"
        cols_geom = "idarea_data, the_geom"
        valuePlaceholder_geom = "%(idarea_data)s, ST_GeometryFromText(%(the_geom)s, " + srid + ")"
        # open shapefile
        #print (path + filename)
        dsShapefile = path + filename
        driver = ogr.GetDriverByName(str('ESRI Shapefile'))
        dataSource = driver.Open(dsShapefile, 0)
        layer = dataSource.GetLayer()
        print('--> layer : '+ str(layer))
        #postgis access
        pg = db_connector()
        pg.dbConfig(db_config)
        pg.dbConnect()
        # iterate over features
        for feature in layer:
            
            #import data and geom to database
            values_data = {}
            values_data["idarea"] = idArea
            values_data["area_data"] = feature.GetField(str(attribute2raster))
            for key in attributes2import:
                if attributes2import[key] == "":
                    values_data[key] = None
                else:
                    values_data[key] = feature.GetField(str(attributes2import[key]))
                    
            idarea_data = pg.tblInsert( table_data, cols_data,  valuePlaceholder_data,  values_data,  returnParameter_data  )
            
            #print('--> idarea_data : '+ str(idarea_data))
            #import geom to database
            values_geom = {}
            values_geom["idarea_data"] = idarea_data
            # geometry as wkt
            values_geom["the_geom"] = feature.GetGeometryRef().ExportToWkt()
            pg.tblInsert( table_geom, cols_geom,  valuePlaceholder_geom,  values_geom,  None  )
            
        layer.ResetReading()
        # close db access
        pg.dbClose()

    def rasterize_pg(self, level, gridLevelConfig, preprocessingConfig, idArea):
        # burn PostgreSQL vector datasource values into a raster
        # resamplingMethod is majority
        print("--> rasterize vector area data from PostgreSQL database")
        r = gdal_raster_functions()
        argDict = {}
        projectDir = preprocessingConfig['general']['projectDir']
        projectName = preprocessingConfig['general']['project']
        inFormat = preprocessingConfig['dataSource']['inFormat']
        #srcPath = preprocessingConfig['dataSource']['srcPath']
        srcEPSG = str(preprocessingConfig['dataSource']['epsg'])
        targetLevelId = str(level)
        targetEPSG = str(preprocessingConfig['rasterize']['epsg'])
        outFormat = preprocessingConfig['rasterize']['outFormat']
        outNoData = preprocessingConfig['rasterize']['nodata']
        
        targetPath = projectDir + projectName + "/areas/" + str(idArea) + "/"
        targetFileName = str(idArea) + '_' + targetLevelId + '.' + outFormat[1] + '.' + str(outFormat[2]) + '.' + outFormat[0]
        print(targetFileName)
        
        gdal_co, gdal_co2 = r.setGDAL_createOptions(outFormat[0])
        of = r.setGDAL_outFormat(outFormat[0])
        tr = r.setGDAL_targetResolution(targetLevelId, gridLevelConfig)
        te = r.setGDAL_targetExtent(targetLevelId, gridLevelConfig)
        
        sql_string = "SELECT idarea_geom AS gid, a.idarea, a.idarea_data as id, a.area_data as name, the_geom FROM spatial.area_data a INNER JOIN spatial.area_geom b ON " +\
            "a.idarea_data = b.idarea_data WHERE a.idarea = " + str(idArea)
            
        dbhost = preprocessingConfig["pg_database"]["db_host"]
        dbname = preprocessingConfig["pg_database"]["db_name"]
        dbuser = preprocessingConfig["pg_database"]["db_user"]
        dbport = preprocessingConfig["pg_database"]["db_port"]
        dbpwd  = preprocessingConfig["pg_database"]["db_password"]
        src_pg = "host="+dbhost+" dbname="+dbname+" user="+dbuser+" password="+dbpwd+" port="+str(dbport)
        
        # create path if not exists
        if not os.path.exists(targetPath):
            os.makedirs(targetPath)
            
        print(targetPath)
        
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
