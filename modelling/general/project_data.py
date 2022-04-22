# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import os, sys, shutil
import gdal
#import ogr
import osr
import subprocess
import glob
import numpy
import h5py
from general.db_connector import db_connector
import json

## class call the server functionen (include DB and Shell)
# @date  24.04.2014
#
# @author Mario Uhlig, Stephan Bürger <br>
#        VisDat geodatentechnologie GmbH <br>
#        01277 Dresden <br>
#        Am Ende 14 <br>
#        http://www.visdat.de <br>
#        info@visdat.de <br>
#
#Die Klasse ProjectData enthält Funktionen für das Schreiben, Konvertieren, Lesen etc. von Daten sowie den fachlichen Modulen zugeordnete Funktionen für deren Datenimport (get_data_soilerosion(), get_data_n_budget() ...).

class ProjectData():

    ## Konstruktor.
    def __init__(self, configObject):
        #projectname, pixelsize, pathtmp, absolute_path, id_raumebene, raum_array, id_szenario, landuse_array
        self.projectname = configObject['general']['projectname']
        self.scenario = configObject['general']['scenario']
        self.years = configObject['general']['years']
        self.minx = configObject['general']['minx']
        self.path_data2viewer = configObject['general']['path_data']
        self.path_data = str(configObject['general']['path_data'])+"/"+str(self.projectname)+"/parameters/"+str(self.scenario)
        self.path_data_area = str(configObject['general']['path_data'])+"/"+str(self.projectname)+"/areas"
        self.miny = configObject['general']['miny']
        self.maxx = configObject['general']['maxx']
        self.maxy = configObject['general']['maxy']
        self.cellsize = configObject['general']['cellsize']
        self.width = configObject['general']['width']
        self.height = configObject['general']['height']
        self.levelId = configObject['general']['levelId']
        self.projection = configObject['general']['projection']
        self.resolution = configObject['general']['resolution']
        self.grid_level_config = configObject['general']['grid_level_config']
        self.epsg = configObject['general']['epsg']
        self.path_preprocessing_exec = configObject['general']['path_preprocessing_exec']

        self.path_tmp = str(configObject['general']['path_data'])+'/'+str(self.projectname)+'/tmp/'

        self.a_spatial_ref = configObject['general']['projection']
        self.a_inverse_flattening = configObject['general']['inverse_flattening']
        self.a_scale_factor_at_central_meridian = configObject['general']['scale_factor_at_central_meridian']
        self.a_false_easting = configObject['general']['false_easting']
        self.a_false_northing = configObject['general']['false_northing']
        self.a_semi_major_axis = configObject['general']['semi_major_axis']
        self.a_latitude_of_projection_origin = configObject['general']['latitude_of_projection_origin']
        self.a_GeoTransform = configObject['general']['GeoTransform']
        self.a_longitude_of_prime_meridian = configObject['general']['longitude_of_prime_meridian']
        self.a_grid_mapping_name = configObject['general']['grid_mapping_name']
        self.a_longitude_of_central_meridian = configObject['general']['longitude_of_central_meridian']

    ## Funktion um JSON-Konfigurationsobjekte aus dem config Ordner zu holen
    #@param self The object pointer.
    #@param modul Konfigurationsbezeichnung, z.B. 'area_import'.
    #@return JSON-Konfigurationsobjekt
    def moduldata_from_txt(self, modul_configfile):

        #configfile_path = self.pathdata+"/"+self.projectname+"/"+str(self.id_szenario)+"/config/config_"+str(self.id_szenario)+"_"+str(config_name)+".json"

        if os.path.isfile(modul_configfile):
            print ('Vorhandene Configdatei (Modul) laden ...')
            data=open(modul_configfile)

            json_data = json.load(data)

            data.close()
        else:
            json_data = []

        return json_data

    def __load_data_area(self, id_area):

        #Area holen
        #zuerst prüfen ob eine numpy-binary Datei des Parameters und Szenarios vorhanden ist
        areafile_path = self.path_data_area+"/"+str(id_area)+"/*.nc"

        for inputFile in glob.glob(areafile_path):

            targetFile = inputFile.replace(self.path_data_area+"/"+str(id_area)+"/", '')

            metaInfos = targetFile.split('.', 1)

            #metafinfos[0] parameterId_level
            #metafinfos[1] datanTyp.Kommastelle.dateiendung

            metaFile = metaInfos[0].split('_')
            metaData = metaInfos[1].split('.')

            if len(metaFile) == 2 and len(metaData) == 3:

                metaAreaId = metaFile[0]
                metaLevel = metaFile[1]

                metaDataTyp = metaData[0]
                metaDataComma = metaData[1]
                metaDataEnd = metaData[2]

                if metaDataEnd == 'nc' and metaLevel == str(self.levelId):

                    if str(id_area) == str(metaAreaId):

                        f = h5py.File(inputFile, 'r')
                        return_array = numpy.array(f['Band1'][:])

                        if '_FillValue' in f['Band1'].attrs:
                            noDataValue =  f['Band1'].attrs['_FillValue'].astype(numpy.float32).item(0)
                        else:
                            noDataValue = 0
                            if metaDataTyp == "int8" or metaDataTyp == "byte":
                                noDataValue = 0
                            if metaDataTyp == "int16":
                                noDataValue = -9999
                            if metaDataTyp == "int32":
                                noDataValue = -99999
                            if metaDataTyp == "int64":
                                noDataValue = -99999
                            if metaDataTyp == "float16":
                                noDataValue = -99999
                            if metaDataTyp == "float32":
                                noDataValue = -99999
                            if metaDataTyp == "float64":
                                noDataValue = -99999

                            print (noDataValue)

                        #print parameterfile_path

                        if return_array.shape[0] == self.height and return_array.shape[1] == self.width:
                            #print return_array.shape

                            #NoDataValue setzen
                            return_array = return_array.astype(numpy.float32)
                            return_array = numpy.where(return_array == noDataValue, numpy.nan, return_array)

                            #Kommastellen bei int erzeugen
                            return_array = return_array / (10**int(metaDataComma))

                            return return_array

                        else:
                            raise ValueError('Extent of '+targetFile+' is unequal to width and height in config; height: '+ str(return_array.shape[0])+' -- '+str(self.height)+' width: '+ str(return_array.shape[1])+' -- '+' ' + str(self.width))


    ## Funktion um Daten aus Filesystem zu holen
    def __load_data(self, id_parameter, year):


        #Parameter holen
        #zuerst prüfen ob eine numpy-binary Datei des Parameters und Szenarios vorhanden ist
        parameterfile_path = self.path_data+"/"+str(id_parameter)+"/"+str(year)+"/*.nc"
        #print parameterfile_path

        for inputFile in glob.glob(parameterfile_path):

            targetFile = inputFile.replace(self.path_data+"/"+str(id_parameter)+"/"+str(year)+"/", '')

            metaInfos = targetFile.split('.', 1)

            #metafinfos[0] parameterId_viewerSzenarioId_level
            #metafinfos[1] datanTyp.Kommastelle.dateiendung

            metaFile = metaInfos[0].split('_')
            metaData = metaInfos[1].split('.')

            if len(metaFile) == 3 and len(metaData) == 3:

                metaParameterId = metaFile[0]
                #metaSzenarioViewer = metaFile[1]
                metaLevel = metaFile[2]

                metaDataTyp = metaData[0]
                metaDataComma = metaData[1]
                metaDataEnd = metaData[2]

                if metaDataEnd == 'nc' and metaLevel == str(self.levelId):

                    if str(id_parameter) == str(metaParameterId):

                        f = h5py.File(inputFile, 'r')
                        return_array = numpy.array(f['Band1'][:])
                        print ('--> load_data ', metaParameterId)
                        if '_FillValue' in f['Band1'].attrs:
                            noDataValue =  f['Band1'].attrs['_FillValue'].astype(numpy.float32).item(0)
                        else:
                            noDataValue = 0
                            if metaDataTyp == "int8" or metaDataTyp == "byte":
                                noDataValue = 0
                            if metaDataTyp == "int16":
                                noDataValue = -9999
                            if metaDataTyp == "int32":
                                noDataValue = -99999
                            if metaDataTyp == "int64":
                                noDataValue = -99999
                            if metaDataTyp == "float16":
                                noDataValue = -99999
                            if metaDataTyp == "float32":
                                noDataValue = -99999
                            if metaDataTyp == "float64":
                                noDataValue = -99999

                            print (noDataValue)


                        #print parameterfile_path

                        if return_array.shape[0] == self.height and return_array.shape[1] == self.width:
                            #print return_array.shape

                            #NoDataValue setzen
                            return_array = return_array.astype(numpy.float32)
                            return_array = numpy.where(return_array == noDataValue, numpy.nan, return_array)

                            #Kommastellen bei int erzeugen
                            return_array = return_array / (10**int(metaDataComma))

                            return return_array

                        else:
                            raise ValueError('Extent of '+targetFile+' is unequal to width and height in config; height: '+ str(return_array.shape[0])+' -- '+str(self.height)+' width: '+ str(return_array.shape[1])+' -- '+' ' + str(self.width))


    ## Daten als numpy_binary in Filesystem schreiben
    #
    #@param self The object pointer
    #@param input_data_array [numpy_array]
    #@param id_parameter [integer] - entsprechender Parametern zum numpy_array
    #
    #Falls im Filesystem schon vorhanen wird das numpy_binary gelöscht und durch das neue ersetzt

    def set_data(self, input_data_array, id_parameter, year, targetFileName, data2Viewer):

        metaInfos = str(targetFileName).split('.', 1)

        #metafinfos[0] parameterId_viewerSzenarioId_level
        #metafinfos[1] datanTyp.Kommastelle.dateiendung

        #metaFile = metaInfos[0].split('_')
        metaData = metaInfos[1].split('.')

        #metaParameterId = metaFile[0]
        #metaSzenarioViewer = metaFile[1]
        #metaLevel = metaFile[2]

        metaDataTyp = metaData[0]
        metaDataComma = metaData[1]
        #metaDataEnd = metaData[2]

        targetPath = self.path_data+"/"+str(id_parameter)+"/"+str(year)
        # filesystem anlegen wenn noch nicht vorhanden
        if os.access(targetPath, os.F_OK) is False:
            os.makedirs(targetPath)

        #parameterfile_path = self.path_data+"/"+str(id_parameter)+"/"+str(year)+"/*.nc"
        netcdfFile = targetPath+"/"+str(targetFileName)
        noDataValue = 0
        if metaDataTyp == "int8" or metaDataTyp == "byte":
            noDataValue = 0
        if metaDataTyp == "int16":
            noDataValue = -9999
        if metaDataTyp == "int32":
            noDataValue = -99999
        if metaDataTyp == "int64":
            noDataValue = -99999
        if metaDataTyp == "float16":
            noDataValue = -99999
        if metaDataTyp == "float32":
            noDataValue = -99999
        if metaDataTyp == "float64":
            noDataValue = -99999

        #Kommastelle berechnen
        if metaDataTyp in ["int8","int16","int32","int64"]:

            input_data_array = input_data_array.astype(numpy.float32) * (10**int(metaDataComma))
        else:
            input_data_array = input_data_array.astype(numpy.float32)

        input_data = numpy.where(numpy.isnan(input_data_array), noDataValue, input_data_array)
        input_data = input_data.astype(str(metaDataTyp))

        if os.path.isfile(netcdfFile):
            print ('Vorhandene Datei loeschen ...')
            os.remove(netcdfFile)

        print ('Datei wird neu erstellt unter: '+str(netcdfFile))

        #def save_as_netcdf4(self, data, levelId, path, fname, nc_gridFile):
        print("-->save as netcdf4... " + netcdfFile)

        print (noDataValue)
        print('input_data -->',input_data.shape)
        #sys.exit()

        # create netcdf data by h5h5netcdf
        
        with h5py.File(netcdfFile, 'w') as f:
            
            #f.dimens.update({'x': self.width})
            #f.dimens.update({'y': self.height})
            f.attrs['Conventions'] = 'CF-1.5'
        
            v = f.create_dataset('Band1',(input_data.shape), dtype=str(metaDataTyp)) #, compression='gzip', compression_opts=9)
            
            v[:,:] = numpy.flip(input_data,0)
            
            v.attrs['grid_mapping'] = 'transverse_mercator'
            v.attrs['_FillValue'] = [noDataValue] #numpy.array(noDataValue).astype(str(metaDataTyp))
            
            vx = f.create_dataset('x', data = numpy.linspace(self.minx+(self.resolution/2), self.maxx-(self.resolution/2), num=self.width))
            vx.attrs['units'] = 'm'
            
            vy = f.create_dataset('y', data = numpy.linspace(self.miny+(self.resolution/2), self.maxy-(self.resolution/2), num=self.height))
            vy.attrs['units'] = 'm'
            
            dt = h5py.special_dtype(vlen=str)
            vm = f.create_dataset('transverse_mercator', (), dtype=dt)
            vm.attrs['spatial_ref'] = self.a_spatial_ref
            vm.attrs['inverse_flattening'] = self.a_inverse_flattening
            vm.attrs['scale_factor_at_central_meridian'] = self.a_scale_factor_at_central_meridian
            vm.attrs['false_easting'] = self.a_false_easting
            vm.attrs['false_northing'] = self.a_false_northing
            vm.attrs['semi_major_axis'] = self.a_semi_major_axis
            vm.attrs['latitude_of_projection_origin'] = self.a_latitude_of_projection_origin
            vm.attrs['GeoTransform'] = self.a_GeoTransform #str(self.minx)+' '+str(self.cellsize)+' 0 '+str(self.miny)+ ' 0 '+str(self.cellsize)
            vm.attrs['longitude_of_prime_meridian'] = self.a_longitude_of_prime_meridian
            vm.attrs['grid_mapping_name'] = self.a_grid_mapping_name
            vm.attrs['longitude_of_central_meridian'] = self.a_longitude_of_central_meridian

        f.close()
        
        """
        with h5netcdf.File(netcdfFile, 'w') as f:
            #print numpy.linspace('huhu')
            f.dimensions.update({'x': self.width})
            f.dimensions.update({'y': self.height})
            f.attrs['Conventions'] = 'CF-1.5'

            v = f.create_variable('x', ('x',), float)
            v[:] = numpy.linspace(self.minx, self.maxx, num=self.width)
            v.attrs['units'] = 'm'
            v = f.create_variable('y', ('y',), float)
            v[:] = numpy.linspace(self.miny, self.maxy, num=self.height)
            v.attrs['units'] = 'm'
            
            v = f.create_variable('Band1', ('y', 'x'), dtype=str(metaDataTyp)) #, compression='gzip', compression_opts=9)
            v[:,:] = input_data
            v.attrs['grid_mapping'] = 'transverse_mercator'
            v.attrs['_FillValue'] = [noDataValue]#numpy.array(noDataValue).astype(str(metaDataTyp))
            dt = h5py.special_dtype(vlen=str)
            v = f.create_variable('transverse_mercator', (), dtype=dt)

            v.attrs['spatial_ref'] = self.a_spatial_ref
            v.attrs['inverse_flattening'] = self.a_inverse_flattening
            v.attrs['scale_factor_at_central_meridian'] = self.a_scale_factor_at_central_meridian
            v.attrs['false_easting'] = self.a_false_easting
            v.attrs['false_northing'] = self.a_false_northing
            v.attrs['semi_major_axis'] = self.a_semi_major_axis
            v.attrs['latitude_of_projection_origin'] = self.a_latitude_of_projection_origin
            v.attrs['GeoTransform'] = self.a_GeoTransform #str(self.minx)+' '+str(self.cellsize)+' 0 '+str(self.miny)+ ' 0 '+str(self.cellsize)
            v.attrs['longitude_of_prime_meridian'] = self.a_longitude_of_prime_meridian
            v.attrs['grid_mapping_name'] = self.a_grid_mapping_name
            v.attrs['longitude_of_central_meridian'] = self.a_longitude_of_central_meridian
        
        f.close()
        """

        if data2Viewer is True:
            
            tempComma = 0
            self.__data2Viewer(targetFileName, id_parameter, year, noDataValue, metaDataTyp, tempComma)
            self.__renameViewerData(targetFileName, id_parameter, year, noDataValue, metaDataTyp, metaDataComma, tempComma)
            #self.__statisticViewer(targetFileName, id_parameter, year)
            #self.__showInViewer(targetFileName, id_parameter, year)
            
    def __data2Viewer(self, targetFileName, id_parameter, year, noDataValue, metaDataTyp, tempComma):
        print (str(targetFileName).replace(str('.nc'), str('')))
        scenario_id = self.get_scenario_id(self.scenario, year)
        
        conf_dict ={
                        "general" : {
                            "project" : self.projectname,
                            "scenario_id" : scenario_id,
                            "scenario_name" : self.scenario,
                            "yearsDef" : str(year),
                            "projectDir" : self.path_data2viewer+'/',
                            "gridLevelConfig" : self.grid_level_config
                        },
                        "process" : "import_parameter",
                    	"dataSource" : {
                    		"inFormat" : "nc",
                            "srcPath" : self.path_data+'/'+str(id_parameter)+'/'+str(year)+'/',
                            "srcName" : str(targetFileName).replace('.nc', ''),
                            "attribute2raster" : "",
                            "nodata" : noDataValue,
                            "epsg" : str(self.epsg)
                        },
                    	"rasterize" : {
                            "idParam" : str(id_parameter),
                            "targetGridLevel" : [self.levelId],
                            "outFormat" : ["nc", metaDataTyp, tempComma],
                            "resamplingMethod" : "near",
                            "epsg" : str(self.epsg),
                            "nodata" : noDataValue
                        },
                        "pg_database" : {
                            "db_host" : "192.168.0.194",
                            "db_name" : "more_uba",
                            "db_user" : "visdat",
                            "db_password" : "9Leravu6",
                            "db_port" : "9991"
                        }
                    }
        
        # write config files
        print ("write config ...")
        
        config_path = self.path_data2viewer+'/'+self.projectname+'/config/processing/parameters/'+self.scenario+'/'+str(year)+'/'
        configFile = config_path+'/'+str(targetFileName)+'.config'
        print (configFile)
        
        if os.access(config_path, os.F_OK) is False:
            os.makedirs(config_path)
            
        with open(configFile, 'w') as file:
             file.write(json.dumps(conf_dict))
             
        print ("run_preprocessing ...")
        print(self.path_preprocessing_exec)
        #sys.exit()
        
        p = subprocess.Popen('/bin/bash', shell=True, stdin=subprocess.PIPE)
        
        #python shell_string.py -p import_parameter -f /mnt/galfdaten/daten_stb/more_uba/config/processing/parameters/base_yearly/2015/100_base_yearly_2015_29_runoff_quotient.config
        shell_string = 'cd '+self.path_preprocessing_exec+'; python3 main.py -p import_parameter -f '+configFile
        
        print (shell_string)
        p.communicate(shell_string.encode('utf8'))[0]
        del p


    def __renameViewerData(self, targetFileName, id_parameter, year, noDataValue, metaDataTyp, metaDataComma, tempComma):

        scenario_id = self.get_scenario_id(self.scenario, year)

        levelFileOrg = self.path_data+'/'+str(id_parameter)+'/'+str(year)+'/'+str(id_parameter)+'_'+str(scenario_id)+'_'+str(self.levelId)+'.'+str(metaDataTyp)+'.'+str(tempComma)+'.nc'
        levelFileTarget = self.path_data+'/'+str(id_parameter)+'/'+str(year)+'/'+str(id_parameter)+'_'+str(scenario_id)+'_'+str(self.levelId)+'.'+str(metaDataTyp)+'.'+str(metaDataComma)+'.nc'

        print ('--> levelFileOrg : '+ str(levelFileOrg))
        print ('--> levelFileTarget : '+ str(levelFileTarget))

        os.rename(levelFileOrg, levelFileTarget)


    def __statisticViewer(self, targetFileName, id_parameter, year):

        


        file_path = self.path_data+'/'+str(id_parameter)+'/'+str(year)
        
        if os.path.isdir(file_path):
            shutil.rmtree(file_path+'/slices/')
        
        #for filename in glob.glob(file_path+'/slices/*baseline*'):
        #    os.remove(filename) 
            
        # checking whether file exists or not
        #if os.path.exists(file_path+'/slices/'):
            # removing the file using the os.remove() method
        #    os.rmdir(file_path+'/slices/')
            
            
        for filename in glob.glob(file_path+"/*baseline*"):
            os.remove(filename) 




        scenario_id = self.get_scenario_id(self.scenario, year)

        idAreaArray = []
        #get area ids for statistic
        connector = db_connector().db_connect()
        cur = connector.cursor()

        try:
            cur.execute("SELECT idarea FROM spatial.area WHERE statistic = true \
                        AND (idarea = 1 OR idarea = 2 OR idarea = 3 OR idarea = 4 OR idarea = 5 OR idarea = 6 \
                    OR idarea = 7 OR idarea = 12 OR idarea = 13 OR idarea = 14 OR idarea = 15) ORDER BY idarea")
        except:
            print ("I can't execute the cursor!")

        rows = cur.fetchall()
        print ("\nRows: \n")
        print (rows)
        for row in rows:
            idAreaArray.append(row[0])

        connector.commit()
        connector.close()


        conf_dict = {
            "process" : "generate_statistics",
            "general" : {
                "statType": ["baselineStatisticV2", "baseStatisticV2"],
                "project" : self.projectname,
                "projectDir" : self.path_data2viewer+'/',
                "cpu_count" : 12,
                "mp_step" : 10000
            },
            "statistics": {
                "idLevel": [self.levelId],
                "idArea": idAreaArray,
                "idScenario" : [scenario_id],
                "idParam" : [id_parameter],
                "groupbyCategory" : [0,1],
                "idCategory" : 6
            },
           "pg_database" : {
               "db_host" : "192.168.0.194",
               "db_name" : "more_uba",
               "db_user" : "visdat",
               "db_password" : "9Leravu6",
               "db_port" : "9991"
           }
        }

        #pruefen ob Pfad Vorhanden - anlegen wenn noch nicht vorhanden
        stat_path = str(self.path_data2viewer+'/'+self.projectname+'/config/processing/statistics/'+str(self.scenario)+'/'+str(year))
        if os.access(stat_path, os.F_OK) is False:
            os.makedirs(stat_path)

        # write config file
        print ("write config ...")

        configFile = str(self.path_data2viewer+'/'+self.projectname+'/config/processing/statistics/'+self.scenario+'/'+str(year)+'/'+str(id_parameter)+'_'+str(scenario_id)+'_'+str(self.levelId)+'.config')
        print (configFile)

        with open(configFile, 'w') as file:
             file.write(json.dumps(conf_dict))

        print ("run_preprocessing ...")

        p = subprocess.Popen('/bin/bash', shell=True, stdin=subprocess.PIPE)

        shell_string = 'cd '+self.path_preprocessing_exec+'; python3 main.py -p generate_statistics -f '+configFile

        print (shell_string)
        p.communicate(shell_string.encode('utf8'))[0]
        del p



    def __showInViewer(self, targetFileName, id_parameter, year):

        scenario_id = self.get_scenario_id(self.scenario, year)

        connector = db_connector().db_connect()
        cur = connector.cursor()

        sqlString = "select * from general.sbvf_insert_param_area_exists(1,"+str(scenario_id)+","+str(id_parameter)+",'"+str(self.path_data2viewer)+"/');select * from general.sbvf_insert_param_area_exists(2,"+str(scenario_id)+","+str(id_parameter)+",'"+str(self.path_data2viewer)+"/');select * from general.sbvf_insert_param_area_exists(3,"+str(scenario_id)+","+str(id_parameter)+",'"+str(self.path_data2viewer)+"/');"
        print (sqlString)
        try:
            cur.execute(sqlString)
        except:
            print ("I can't execute the cursor!")

        #ows = cur.fetchall()

        connector.commit()
        connector.close()


    def get_data_p_budget(self, function_name, year, conf):

        input_data = {}

        #Landnutzung
        #input_data['model_area'] = self.__load_data_area(1)
        
        
        
        if function_name == 'union_schluff':
            
            input_data['deutschland'] = self.__load_data(2441, year)
            input_data['sachsen'] = self.__load_data(2440, year)
            return input_data
        
        if function_name == 'union_ton':
            
            input_data['deutschland'] = self.__load_data(2431, year)
            input_data['sachsen'] = self.__load_data(2430, year)
            return input_data
        
        if function_name == 'union_humus':
            
            input_data['deutschland'] = self.__load_data(3361, year)
            input_data['sachsen'] = self.__load_data(3360, year)
            return input_data
        
        if function_name == 'sdr':
            
            input_data['water_distance'] = self.__load_data(20, year)
            input_data['abag'] = self.__load_data(48, year)
            input_data['sedimenteintrag'] = self.__load_data(53, year)
            return input_data
        
        if function_name == 'pt_boden':
            
            input_data['landuse'] = self.__load_data(6, year)
            input_data['pt_boden_1'] = self.__load_data(901, year)
            input_data['pt_boden_2'] = self.__load_data(902, year)
            input_data['pt_boden_3'] = self.__load_data(903, year)
            input_data['pt_boden_4'] = self.__load_data(904, year)
            input_data['pt_boden_5'] = self.__load_data(905, year)
            input_data['pt_boden_6'] = self.__load_data(906, year)
            input_data['pt_boden_7'] = self.__load_data(907, year)
            input_data['pt_boden_11'] = self.__load_data(911, year)
            input_data['pt_boden_13'] = self.__load_data(913, year)
            
            return input_data
        
        if function_name == 'fg_boden':

            input_data['ton'] = self.__load_data(243, year)
            input_data['schluff'] = self.__load_data(244, year)
            return input_data
        
        if function_name == 'enr_fein':

            input_data['fg_boden'] = self.__load_data(245, year)
            input_data['sdr'] = self.__load_data(60, year)
            return input_data
        
        if function_name == 'c_sed':

            input_data['pt_boden'] = self.__load_data(9, year)
            input_data['enr_fein'] = self.__load_data(246, year)
            return input_data
        
        if function_name == 'ppart':

            input_data['landuse'] = self.__load_data(6, year)
            input_data['sediment'] = self.__load_data(53, year)
            input_data['c_sed'] = self.__load_data(72, year)
            input_data['p_eros_off'] = self.__load_data(73, year)
            return input_data
        
        if function_name == 'p_diff':
            
            input_data['landuse'] = self.__load_data(6, year)
            input_data['p_ag'] = self.__load_data(68, year)
            input_data['p_ao'] = self.__load_data(69, year)
            input_data['p_draen'] = self.__load_data(71, year)
            input_data['p_part'] = self.__load_data(74, year)
            input_data['p_atm_gew'] = self.__load_data(61, year)
            
            return input_data
        
        if function_name == 'p_urban':
            
            input_data['p_ka_50'] = self.__load_data(63, year)
            input_data['p_ka_50_2000'] = self.__load_data(62, year)
            input_data['p_mka'] = self.__load_data(64, year)
            input_data['p_tka'] = self.__load_data(65, year)
            
            return input_data
        
        if function_name == 'p_sied_ges':
            
            input_data['landuse'] = self.__load_data(6, year)
            input_data['p_ka'] = self.__load_data(82, year)
            input_data['p_urban'] = self.__load_data(76, year)
            
            return input_data
            
        if function_name == 'pges':
            
            input_data['landuse'] = self.__load_data(6, year)
            input_data['p_ka'] = self.__load_data(82, year)
            input_data['p_urban'] = self.__load_data(76, year)
            input_data['p_diff'] = self.__load_data(75, year)
            
            return input_data
        
        if function_name == 'ppart_test':
            input_data['landuse'] = self.__load_data(6, year)
            input_data['ppart_1410'] = self.__load_data(48, year)
            input_data['ppart_1411'] = self.__load_data(53, year)
            input_data['ppart_1412'] = self.__load_data(20, year)
            input_data['ppart_1413'] = self.__load_data(21, year)
            input_data['ppart_1414'] = self.__load_data(22, year)
            input_data['ppart_1415'] = self.__load_data(23, year)
            input_data['ppart_1416'] = self.__load_data(24, year)
            input_data['ppart_1417'] = self.__load_data(243, year)
            input_data['ppart_1418'] = self.__load_data(244, year)
            
            return input_data
        

    def resamplingGrid(self, inputFile, targetFile, resamplingMethod, xmin, ymin, xmax, ymax):

        p = subprocess.Popen('/bin/bash', shell=True, stdin=subprocess.PIPE)
        saga_string = 'saga_cmd grid_tools "Resampling" -INPUT=' + inputFile + ' -OUTPUT=' + targetFile + ' -KEEP_TYPE=1 -SCALE_UP='+str(resamplingMethod)+' -SCALE_DOWN='+str(resamplingMethod)+' -TARGET_USER_FITS=1 -TARGET_USER_SIZE=' + str(self.cellsize) + ' -TARGET_USER_XMIN=' + str(xmin) + ' -TARGET_USER_XMAX=' + str(xmax) + ' -TARGET_USER_YMIN=' + str(ymin) + ' -TARGET_USER_YMAX=' + str(ymax)
        
        print (saga_string)
        
        p.communicate(saga_string.encode('utf8'))[0]
        del p
        
        return 'ok'


    ## Rastern (GeoTiff) einer Shapedatei
    #
    #@param self The object pointer
    #@param id_parameter [integer]
    #@param attribute [text]
    #
    # unter Verwendung von gdal_rasterize

    def rasterize_data(self, id_parameter, attribute):

        print ('rasterize_data ...')

        shellcall = 'gdal_rasterize -a '+attribute+' -l '+str(id_parameter)+' '+self.pathtmp+str(id_parameter)+'.shp '+self.pathtmp+str(id_parameter)+'.tif'
        print (shellcall)
        os.system(shellcall)

    ## Raster in ein numpy-array wandeln
    #
    #@param self The object pointer
    #@param id_parameter [integer]
    #@return numpy_array

    def raster2numpy(self, rasterData):

        ds = gdal.Open(rasterData)
        myarray = numpy.array(ds.GetRasterBand(1).ReadAsArray())

        ulx, xres, xskew, uly, yskew, yres = ds.GetGeoTransform()
        #lrx = ulx + (ds.RasterXSize * xres)
        #lry = uly + (ds.RasterYSize * yres)

        xmin = ulx
        ymax = uly

        myarray = numpy.flipud(myarray)

        return myarray, xmin, ymax

    def numpy2raster(self, xmin, ymax, cellsize, numpyArray, rasterData):

        numpyArray = numpy.flipud(numpyArray)

        drv = gdal.GetDriverByName(str("SAGA"))
        ds = drv.Create(rasterData, int(numpyArray.shape[1]), int(numpyArray.shape[0]), int(1), gdal.GDT_Float32)
        ds.GetRasterBand(1).SetNoDataValue(-99999)

        export_array = numpy.where(numpyArray == numpy.nan, -99999, numpyArray)

        ds.GetRasterBand(1).WriteArray(export_array.astype(numpy.float32))

        srs = osr.SpatialReference()
        srs.ImportFromEPSG(self.epsg)

        ds.SetProjection(srs.ExportToWkt())

        geo_transform = [xmin, cellsize, 0, ymax, 0, -(cellsize)]
        ds.SetGeoTransform(geo_transform)


    def get_scenario_id(self, scenario_name, year):

        connector = db_connector().db_connect()
        cur = connector.cursor()

        retVal = -99999

        try:
            sqlTxt = "SELECT get_id_spatial_scenario AS id FROM spatial.get_id_spatial_scenario('more_uba', '"+str(scenario_name)+"', '"+str(year)+"')"
            #print sqlTxt
            cur.execute(sqlTxt)
        except:
            print ("I can't execute the cursor!")

        rows = cur.fetchall()

        for row in rows:
            retVal = row[0]
            #idAreaArray.append(row[0])
        connector.commit()
        connector.close()

        return retVal