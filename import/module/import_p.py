import os, sys
#import subprocess
import h5py
import numpy as np
from datetime import datetime
sys.path.append('/mnt/galfdaten/Programmierung/tools/more_uba/import/module')
sys.path.append('/mnt/galfdaten/Programmierung/tools/more_uba/import/pg')
from pg import db_connector

class import_p():
    
    def __init__(self):
        print("class import_p")
        
    def doProcessing(self, dataConfig):
        
        np_all = None
        
        print("--> calculate import_p")
        
        
        
        generalDefs = dataConfig["general"]
        statisticsDefs = dataConfig["statistics"]
        ncsDefs = dataConfig["nc"]
        db_config = dataConfig["pg_database"]
        
        #print('--> dataConfig : '+str(dataConfig))
        #print('--> ncsDefs : '+str(ncsDefs))
        #sys.exit()
        
        
        print('-->db_config : '+str(db_config))
        
        
        print('--> statisticsDef : '+str(statisticsDefs))
        
        pg = db_connector()
        pg.dbConfig(db_config)
        
        for param in statisticsDefs['idParamImport']:
            
            print("------------------------------")
            print("--> current configuration")
            print('--> param : ' + str(param))
            
            idParam = param['idParam']
            subCategory = param['idSubCategory']
            
            print('--> idParam : ' + str(idParam))
            print('--> subCategory : ' + str(subCategory))
            
            
            idscenario = statisticsDefs['idScenario']
            idlevel = statisticsDefs['idLevel']
            idcategory = statisticsDefs['idCategory']
            
            pg.dbConnect()
            record,  rowcount = pg.tblSelect( "SELECT idsz, model_scenario_name, model_year, model_project_name  FROM spatial.scenario WHERE idsz = "+str(idscenario))
            #record,  rowcount = pg.tblSelect( "SELECT idsc AS idsz, model_scenario_name, 'static' AS model_year, model_project_name  FROM spatial.scenario WHERE idsc = "+str(scenario))
            pg.dbClose()
            
            print('-->record : '+str(record))
            print('-->rowcount : '+str(rowcount))
            
            currentScenarioList = list( record[0])
            currentScenario = {}
            currentScenario = {'scenario_id' : currentScenarioList[0], 'scenario_name' : currentScenarioList[1], 'yearsDef' : currentScenarioList[2], 'project' : currentScenarioList[3], 'projectDir' : generalDefs['projectDir']}
            print('--> currentScenario : ' + str(currentScenario))
            
            
            currentDef = {}
            currentDef = {'idScenario' : idscenario, 'idParam' : idParam, 'idLevel' : idlevel, 'idCategory' : idcategory}
            print('--> currentDef : ' + str(currentDef))
            configParam, configCategory = self.check_files(currentDef, generalDefs, currentScenario)
            
            print('--> configParam : ' + str(configParam))
            print('--> configCategory : ' + str(configCategory))
            
            if configParam['h5py_file'] == -1:
                print("loading parameter or param file failed")
                break;
                #sys.exit()
            if configCategory['h5py_file'] == -1:
                print("loading parameter or category file failed")
                sys.exit()
                
            if configParam['h5py_file'] != -1 and configCategory['h5py_file'] != -1:
                
                f_param_array = []
                f_param_array = configParam['h5py_file'], str(configParam['decimals']), str(configParam['fpath']), str(configParam['fname']), str(configParam['dtype'])
                f_category_array = []
                f_category_array = configCategory['h5py_file'], str(configCategory['decimals']), str(configCategory['fpath']), str(configCategory['fname']), str(configCategory['dtype'])
                
                print('--> f_param_array : ' + str(f_param_array))
                print('--> f_category_array : ' + str(f_category_array))
                
                f_param = f_param_array[0]
                
                print('--> 6')
                param_band1 = f_param['Band1']
                
                del f_param
                
                print('--> 1')
                noData_param = param_band1.attrs['_FillValue']
                
                print('--> 2')
                np_param = np.array(param_band1).astype(float)
                
                del param_band1
                
                print('--> 3')
                np_param[np_param == noData_param] = np.nan
                
                print('--> 4')
                np_param = np_param.astype('int32')
                
                print('--> 5')
                f_category = f_category_array[0]
                
                print('--> 6')
                category_band1 = f_category['Band1']
                
                del f_category
                
                print('--> 7')
                noData_category = category_band1.attrs['_FillValue']
                
                print('--> 8')
                np_category = np.array(category_band1).astype(float)
                
                del category_band1
                
                print('--> 9')
                np_category[np_category == noData_category] = np.nan
                
                print('--> 10')
                np_category = np_category.astype('int32')
                
                print('--> 11')
                np_ar = np.stack((np_param, np_category))
                
                del np_param
                del np_category
                
                print('--> 12')
                #np_ar = np.where(np.isnan(np_ar[1]) == 1, np_ar[0], np.nan)
                
                print('--> subCategory', subCategory)
                
                for idSubCategory in subCategory:
                    
                    print('--> idSubCategory', idSubCategory)
                    
                    np_ges = np.where(np_ar[1] == idSubCategory, np_ar[0], np.nan)
                
                    print('--> 13')
                    
                    if np_all is None:
                        print('--> 14')
                        np_all = np_ges
                    else:
                        print('--> 15')
                        np_ges_2 = np.stack((np_all, np_ges))
                        
                        del np_all
                        print('--> 16')
                        np_all = np.where(np.isnan(np_ges_2[0]) == 1, np_ges_2[1],  np_ges_2[0])
                        
                        print('--> 17')
                        del np_ges_2
                        
                        del np_ges
                        
                del np_ar
        
        print('--> 18')
        np_all = np.where(np.isnan(np_all), noData_param, np_all)
        print('--> 19')
        
        #print('--> currentScenario : ' + str(currentScenario))
        
        idScenario = str(currentScenario['scenario_id'])
        scenarioName = str(currentScenario['scenario_name'])
        modelYear = str(currentScenario['yearsDef'])
        projectName = str(currentScenario['project'])
        baseDir = str(currentScenario['projectDir'])
        
        curr_dt = datetime.now()

            
        path = baseDir + projectName + '/parameters/' + scenarioName + '/100/' + modelYear + '/'
        fname =  '1000_' + idScenario+ '_' + str(idlevel) + str(int(round(curr_dt.timestamp())))
        file = path + fname + '_neu.nc'
        
        #with h5py.File(file,'r+') as ds:
        #    ds['Band1'][:] = np_all
            
            
        with h5py.File(file, 'w') as f:
            #f = h5py.File(file, 'w')
            f.attrs['Conventions'] = 'CF-1.5'
            
            #ds = f.create_dataset('Band1',data=np_all) #, compression='gzip', compression_opts=9)
            ds = f.create_dataset('Band1', (np_all.shape) , dtype=np_all.dtype) #, compression='gzip', compression_opts=9)
            
            print('--> 20')
            
            ds[:,:] = np.flip(np_all,0)
            
            print('--> 21')
            
            print('--> configParam : ' + str(configParam))
            

            #ds.attrs['DIMENSION_LIST'] = ["(1-3356)", "(1-1372)"]  
            #ds.attrs['DIMENSION_LIST'] = configParam['DIMENSION_LIST']
            
            #print(ds.attrs['DIMENSION_LIST'].values())
            #sys.exit()
            
            #ds.attrs['DIMENSION_LIST'][0] = configParam['DIMENSION_LIST'][0]
            #ds.attrs['DIMENSION_LIST'][1] = configParam['DIMENSION_LIST'][1]
            
            #numcells= []
            #numcells[0] = [230,230,30]
            #ds.attrs['DIMENSION_LIST'].modify('num_cells',numcells)
            
            ds.attrs['_FillValue'] = [ncsDefs['Band1']['_FillValue']]
            
            #ds..update().dims[0].label = 'm'
            #ds.dims[1].label = 'n
            ds.attrs['grid_mapping'] = ncsDefs['Band1']['grid_mapping']
            ds.attrs['long_name'] = ncsDefs['Band1']['long_name']
            
            dt = h5py.special_dtype(vlen=str)
            vm = f.create_dataset('transverse_mercator', (), dtype=dt)
            vm.attrs['GeoTransform'] = ncsDefs['transverse_mercator']['GeoTransform']
            vm.attrs['false_easting'] = [ncsDefs['transverse_mercator']['false_easting']]
            vm.attrs['false_northing'] = [ncsDefs['transverse_mercator']['false_northing']]
            vm.attrs['grid_mapping_name'] = ncsDefs['transverse_mercator']['grid_mapping_name']
            vm.attrs['inverse_flattening'] = [ncsDefs['transverse_mercator']['inverse_flattening']]
            vm.attrs['latitude_of_projection_origin'] = [ncsDefs['transverse_mercator']['latitude_of_projection_origin']]
            vm.attrs['long_name'] = ncsDefs['transverse_mercator']['long_name']
            vm.attrs['longitude_of_central_meridian'] = [ncsDefs['transverse_mercator']['longitude_of_central_meridian']]
            vm.attrs['longitude_of_prime_meridian'] = [ncsDefs['transverse_mercator']['longitude_of_prime_meridian']]
            vm.attrs['scale_factor_at_central_meridian'] = [ncsDefs['transverse_mercator']['scale_factor_at_central_meridian']]
            vm.attrs['semi_major_axis'] = [ncsDefs['transverse_mercator']['semi_major_axis']]
            vm.attrs['spatial_ref'] = ncsDefs['transverse_mercator']['spatial_ref']
            
            
            vx = f.create_dataset('x', data = np.linspace(ncsDefs['x']['minx'], ncsDefs['x']['maxx'], ncsDefs['x']['width']))
            vy = f.create_dataset('y', data = np.linspace(ncsDefs['y']['miny'], ncsDefs['y']['maxy'], ncsDefs['y']['height']))
            
            #vx = f.create_dataset('x', (), dtype=dt)
            vx.attrs['CLASS'] = ncsDefs['x']['CLASS']
            vx.attrs['NAME'] = ncsDefs['x']['NAME']
            #vx.attrs['REFERENCE_LIST'] = "[{1-474,1}]"
            #vx.attrs['REFERENCE_LIST'] = configParam['x']
            #vx.attrs['REFERENCE_LIST'][0] = configParam['x'][0]
            #vx.attrs['REFERENCE_LIST'][1] = configParam['x'][1]
            vx.attrs['_Netcdf4Dimid'] = [ncsDefs['x']['_Netcdf4Dimid']]
            vx.attrs['long_name'] = ncsDefs['x']['long_name']
            vx.attrs['standard_name'] = ncsDefs['x']['standard_name']
            vx.attrs['units'] = ncsDefs['x']['units']
            
            #vy = f.create_dataset('y', (), dtype=dt)
            vy.attrs['CLASS'] = ncsDefs['y']['CLASS']
            vy.attrs['NAME'] = ncsDefs['y']['NAME']
            #vy.attrs['REFERENCE_LIST'] = configParam['y']
            #vy.attrs['REFERENCE_LIST'] = {1-474,1}
            #vy.attrs['REFERENCE_LIST'] =configParam['y']
            vy.attrs['_Netcdf4Dimid'] = [ncsDefs['y']['_Netcdf4Dimid']]
            vy.attrs['long_name'] = ncsDefs['y']['long_name']
            vy.attrs['standard_name'] = ncsDefs['y']['standard_name']
            vy.attrs['units'] = ncsDefs['y']['units']
            
            
            
            #vx = f.create_dataset('x', data = numpy.linspace(self.minx, self.maxx, num=self.width))
            #vx.attrs['units'] = 'm'
            
            #f.create_dataset('x', np_ar = x)
            #f.create_dataset('y', np_ar = y)
            
            #x = f.create_dataset('x', data='')
            #y=  f.create_dataset('y', data='')
            
            #sys.exit()
            
            #ds[:] = np_all
        f.close()


    def check_files(self, statDef, generalDefs, currentScenario):
        #print("check files")
        f_param, f_category = -1, -1
        
        if statDef['idParam'] != "":
            f_param = self.check_paramFile(generalDefs, currentScenario, str(statDef['idParam']), str(statDef['idLevel']))
        else:
            print("missing parameter id")
            sys.exit()
            
        if statDef['idCategory'] != "":
            f_category = self.check_categoryFile(generalDefs, currentScenario, str(statDef['idCategory']), str(statDef['idLevel']))
        else:
            print("missing category id")
            sys.exit()
            
            
        return f_param, f_category

    def check_paramFile(self, Defs, DefScenario, idParam, idLevel):
        #print("check parameter file")
        f_param = -1
        decimals = 0
        dataType = None
        config = {}
        #currNc = {}
        
        idScenario = str(DefScenario['scenario_id'])
        scenarioName = str(DefScenario['scenario_name'])
        modelYear = str(DefScenario['yearsDef'])
        projectName = str(DefScenario['project'])
        baseDir = str(DefScenario['projectDir'])
        
        path = baseDir + projectName + '/parameters/' + scenarioName + '/' + idParam + '/' + modelYear + '/'
        fname = idParam + '_' + idScenario + '_' + idLevel
        file = path + fname
        
        if os.path.exists(path):
            for x in os.listdir(path):
                xDef = x.split('.')
                
                if len(xDef) == 4 and xDef[0] == fname and xDef[1] != 'baseline':
                    dataType = xDef[1]
                    decimals = xDef[2]
                    fileFormat = xDef[3]
                    file = file + '.' + str(dataType) + '.' + str(decimals) + '.' + str(fileFormat)
                    
                    config['f_file'] = str(file)
                    config['fname'] = fname
                    config['fpath'] = path
                    config['dtype'] = dataType
                    config['fFormat'] = fileFormat
                    config['decimals'] = decimals
                    config['idParm'] = idParam
                    
            if os.path.isfile(file):
                f_param = h5py.File(file, 'r')
                
                config['h5py_file'] = f_param
                ds = f_param['Band1']
                config['fillValue']  = ds.attrs['_FillValue'][0]
                #print('--> area file found: ' + file)
                
                config['DIMENSION_LIST']  = ds.attrs['DIMENSION_LIST']
                config['x']  = f_param['x'].attrs['REFERENCE_LIST']
                config['y']  = f_param['y'].attrs['REFERENCE_LIST']
                #print('--> parameter file found: ' + file)
            else:
                config['h5py_file'] = -1
                print('--> missing parameter file (h5 or nc): ' + file)
        else:
            config['h5py_file'] = -1
            print('--> parameter path not found: ' + str(path))
            
        print('--> config: ',config)
        #sys.exit()
            
        return config

    def check_areaFile(self, Defs, DefScenario, idArea, idLevel):
        #print("check area file")
        f_area = -1
        decimals = 0
        config = {}
        
        #idScenario = str(DefScenario['scenario_id'])
        #scenarioName = str(DefScenario['scenario_name'])
        #modelYear = str(DefScenario['yearsDef'])
        projectName = str(DefScenario['project'])
        baseDir = str(DefScenario['projectDir'])
        
        path = baseDir + projectName + '/areas/' + idArea + '/'
        fname = idArea + '_' + idLevel
        file = path + fname
        
        if os.path.exists(path):
            for x in os.listdir(path):
                xDef = x.split('.')
                if len(xDef) == 4 and xDef[0] == fname:
                    dataType = xDef[1]
                    decimals = xDef[2]
                    fileFormat = xDef[3]
                    file = file + '.' + str(dataType) + '.' + str(decimals) + '.' + str(fileFormat)
                    
                    config['f_file'] = file
                    config['fname'] = fname
                    config['fpath'] = path
                    config['dtype'] = dataType
                    config['fFormat'] = fileFormat
                    config['decimals'] = decimals
                    config['idArea'] = idArea
                    
            if os.path.isfile(file):
                f_area = h5py.File(file, 'r')
                config['h5py_file'] = f_area
                ds = f_area['Band1']
                config['fillValue']  = ds.attrs['_FillValue'][0]
                #print('--> area file found: ' + file)
            else:
                config['h5py_file'] = -1
                print('--> missing area file (h5 or nc): ' + file)
        else:
            config['h5py_file'] = -1
            print('--> area path not found: ' + str(path))
            
        return config

    def check_categoryFile(self, Defs, DefScenario, idCategory, idLevel):
        #print("check category file")
        f_category = -1
        decimals = 0
        config = {}
        
        idScenario = str(DefScenario['scenario_id'])
        scenarioName = str(DefScenario['scenario_name'])
        modelYear = str(DefScenario['yearsDef'])
        projectName = str(DefScenario['project'])
        baseDir = str(DefScenario['projectDir'])
        
        path = baseDir + projectName + '/parameters/' + scenarioName + '/' + idCategory + '/' + modelYear + '/'
        fname = idCategory + '_' + idScenario + '_' + idLevel
        file = path + fname
        
        if os.path.exists(path):
            for x in os.listdir(path):
                xDef = x.split('.')
                if len(xDef) == 4 and xDef[0] == fname:
                    dataType = xDef[1]
                    decimals = xDef[2]
                    fileFormat = xDef[3]
                    file = file + '.' + str(dataType) + '.' + str(decimals) + '.' + str(fileFormat)
                    
                    config['f_file'] = file
                    config['fname'] = fname
                    config['fpath'] = path
                    config['dtype'] = dataType
                    config['fFormat'] = fileFormat
                    config['decimals'] = decimals
                    config['idCategory'] = idCategory
                    
            if os.path.isfile(file):
                f_category = h5py.File(file, 'r')
                config['h5py_file'] = f_category
                ds = f_category['Band1']
                config['fillValue']  = ds.attrs['_FillValue'][0]
                #print('--> category file found: ' + file)
            else:
                config['h5py_file'] = -1
                print('--> missing category file (h5 or nc): ' + file)
        else:
            config['h5py_file'] = -1
            print('--> category path not found: ' + str(path))
            
        return config
