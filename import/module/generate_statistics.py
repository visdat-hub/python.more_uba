import os, sys
#import subprocess
import h5py
sys.path.append('/mnt/galfdaten/Programmierung/tools/more_uba/import/module')
sys.path.append('/mnt/galfdaten/Programmierung/tools/more_uba/import/pg')
from generate_statistics_basestats_v1 import generate_statistics_basestats_v1
from generate_statistics_basestats_v2 import generate_statistics_basestats_v2
from generate_statistics_baseline_v2 import generate_statistics_baseline_v2
from pg import db_connector
#from module.statistics_save import statistics_save

class generate_statistics():
    
    def __init__(self):
        print("class generate_statistics")
        
    def do_statistics(self, dataConfig):
        print("--> calculate statistics")
        
        generalDefs = dataConfig["general"]
        statisticsDefs = dataConfig["statistics"]
        db_config = dataConfig["pg_database"]
        print('-->db_config : '+str(db_config))
        
        for statType in generalDefs['statType']:
            
            if statType == "baselineStatisticV2":
                
                print("--> baselineStatisticV2")
                b = generate_statistics_baseline_v2()
                
                mp_step = generalDefs['mp_step']
                
                pg = db_connector()
                pg.dbConfig(db_config)
                
                for scenario in statisticsDefs['idScenario']:
                    
                    pg.dbConnect()
                    record,  rowcount = pg.tblSelect( "SELECT idsz, model_scenario_name, model_year, model_project_name  FROM spatial.scenario WHERE idsz = "+str(scenario))
                    pg.dbClose()
                    
                    print('-->record : '+str(record))
                    print('-->rowcount : '+str(rowcount))
                    
                    currentScenarioList = list( record[0])
                    currentScenario = {}
                    currentScenario = {'scenario_id' : currentScenarioList[0], 'scenario_name' : currentScenarioList[1], 'yearsDef' : currentScenarioList[2], 'project' : currentScenarioList[3], 'projectDir' : generalDefs['projectDir']}
                    print('--> currentScenario : ' + str(currentScenario))
                    
                    for level in statisticsDefs['idLevel']:
                        
                        for groupbyCategory in statisticsDefs['groupbyCategory']:
                            
                            if groupbyCategory == 1:
                                
                                for level in statisticsDefs['idLevel']:
                                    
                                    print('--> level :' + str(level))
                                    print('--> idCategory :' +  str(statisticsDefs['idCategory']))
                                    print('--> generalDefs :' + str(generalDefs))
                                    
                                    config = self.check_categoryFile(generalDefs, currentScenario, str(statisticsDefs['idCategory']), str(level))
                                    config['id'] = str(groupbyCategory)
                                    
                                    if config['h5py_file'] == -1:
                                        print("loading parameter or category file failed")
                                        sys.exit()
                                        
                                    print('-->config : '+str(config))
                                    
                                    # create data slices if not exist
                                    b.writing_slices2h5('category', config, level, mp_step)
                                    
                                    
                        for param in statisticsDefs['idParam']:
                            
                            config = self.check_paramFile(generalDefs, currentScenario, str(param), str(level))
                            config['id'] = str(param)
                            
                            if config['h5py_file'] == -1:
                                print("loading parameter or category file failed")
                                sys.exit()
                                
                            # create data slices if not exist
                            b.writing_slices2h5('param', config, level, mp_step)
                            
                            
                        for area in statisticsDefs['idArea']:
                            
                            config = self.check_areaFile(generalDefs, currentScenario, str(area), str(level))
                            config['id'] = str(param)
                            
                            if config['h5py_file'] == -1:
                                print("loading parameter or category file failed")
                                sys.exit()
                                
                            # create data slices if not exist
                            b.writing_slices2h5('area', config, level, mp_step)
                            
        pg = db_connector()
        pg.dbConfig(db_config)
        
        for scenario in statisticsDefs['idScenario']:
            
            pg.dbConnect()
            record,  rowcount = pg.tblSelect( "SELECT idsz, model_scenario_name, model_year, model_project_name  FROM spatial.scenario WHERE idsz = "+str(scenario))
            #record,  rowcount = pg.tblSelect( "SELECT idsc AS idsz, model_scenario_name, 'static' AS model_year, model_project_name  FROM spatial.scenario WHERE idsc = "+str(scenario))
            pg.dbClose()
            
            #print('-->record : '+str(record))
            #print('-->rowcount : '+str(rowcount))
            
            currentScenarioList = list( record[0])
            
            currentScenario = {}
            currentScenario = {'scenario_id' : currentScenarioList[0], 'scenario_name' : currentScenarioList[1], 'yearsDef' : currentScenarioList[2], 'project' : currentScenarioList[3], 'projectDir' : generalDefs['projectDir']}
            #print('--> currentScenario : ' + str(currentScenario))
            
            for param in statisticsDefs['idParam']:
                
                for level in statisticsDefs['idLevel']:
                    
                    for area in statisticsDefs['idArea']:
                        
                        for groupbyCategory in statisticsDefs['groupbyCategory']:
                            
                            for statType in generalDefs['statType']:
                                
                                idCategory = statisticsDefs['idCategory']
                                
                                #print("------------------------------")
                                #print("--> current configuration")
                                
                                currentDef = {}
                                currentDef = {'statType' : statType, 'idScenario' : scenario, 'idParam' : param, 'idArea' : area, 'idLevel' : level, 'idCategory' : idCategory, 'groupbyCategory' : groupbyCategory }
                                #print('--> currentDef : ' + str(currentDef))
                                configParam, configCategory, configArea = self.check_files(currentDef, generalDefs, currentScenario)
                                #print('--> configParam : ' + str(configParam))
                                #print('--> configCategory : ' + str(configCategory))
                                #print('--> configArea : ' + str(configArea))
                                
                                if configParam['h5py_file'] == -1:
                                    print("loading parameter or param file failed")
                                    break;
                                    #sys.exit()
                                if configCategory['h5py_file'] == -1:
                                    print("loading parameter or category file failed")
                                    sys.exit()
                                if configArea['h5py_file'] == -1:
                                    print("loading parameter or area file failed")
                                    sys.exit()
                                    
                                if configParam['h5py_file'] != -1 and configArea['h5py_file'] != -1 and configCategory['h5py_file'] != -1:
                                    
                                    f_param_array = []
                                    f_param_array = configParam['h5py_file'], str(configParam['decimals']), str(configParam['fpath']), str(configParam['fname']), str(configParam['dtype'])
                                    f_category = []
                                    f_category = configCategory['h5py_file']
                                    f_area_array = []
                                    f_area_array = configArea['h5py_file'], str(configArea['decimals']), str(configArea['fpath']), str(configArea['fname']), area
                                    
                                    #print('--> f_param_array : ' + str(f_param_array))
                                    #print('--> f_category : ' + str(f_category))
                                    #print('--> f_area_array : ' + str(f_area_array))
                                    
                                    if statType == "baseStatisticV1":
                                        
                                        if groupbyCategory == 0:
                                            f_category = -1
                                            
                                        s = generate_statistics_basestats_v1()
                                        s.run(f_param_array, f_category, f_area_array, 0, 'avg')
                                        
                                    if statType == "baseStatisticSumV1":
                                        
                                        if groupbyCategory == 0:
                                            f_category = -1
                                            
                                        s = generate_statistics_basestats_v1()
                                        s.run(f_param_array, f_category, f_area_array, 0, 'sum')
                                        
                                    if statType == "baselineStatisticV2":
                                        
                                        print("--> baseline_statistics_slices")
                                        
                                        slice_defs = []
                                        pathArea = configArea['fpath']
                                        pathCategory = configCategory['fpath']
                                        pathParam = configParam['fpath']
                                        fnameCategory = configCategory['fname']
                                        fnameParam = configParam['fname']
                                        #print("--> pathArea : " + str(pathArea))
                                        #print("--> pathCategory : " + str(pathCategory))
                                        #print("--> pathParam : " + str(pathParam))
                                        #print("--> configParam : " + str(configParam))
                                        
                                        print("--> run_slices() : starting baseline statistics for area " + str(area))
                                        #######################################################################
                                        # loop through data slices of a parameter
                                        #######################################################################
                                        for dirname, dirs, files in os.walk(pathArea + '/slices/' + str(level)):
                                            for i in files:
                                                split_fname = i.split('.')
                                                slice = split_fname[3]
                                                #dtype = split_fname[1]
                                                
                                                slice_defs.append({
                                                    'area' : {
                                                        'idArea' : area,
                                                        'idLevel' : level,
                                                        'fpath' : dirname + '/' + i,
                                                        'groupbyCategory' : str(groupbyCategory),
                                                        'slice' : slice,
                                                        'fillValue' : configArea['fillValue']
                                                    },
                                                    'parameter' :{
                                                        'idParam' : param,
                                                        'fpath' : pathParam + 'slices/' + str(level) + '/' + fnameParam + '.' + configParam['dtype'] + '.' + str(configParam['decimals']) + '.' + slice + '.h5',
                                                        'fillValue' : configParam['fillValue']
                                                    },
                                                    'category' :{
                                                        'idCategory' : idCategory,
                                                        'fpath' : pathCategory + 'slices/' + str(level) + '/' + fnameCategory + '.'  + configCategory['dtype'] + '.' + str(configCategory['decimals']) + '.' + slice + '.h5',
                                                        'fillValue' : configCategory['fillValue']
                                                    },
                                                    'configParam' : configParam,
                                                    'configCategory' : configCategory,
                                                    'configArea' : configArea,
                                                    
                                                })
                                                
                                        #print ('--> slice_defs : ')
                                        #print (slice_defs)
                                        #print ('--> configParam : '+str(configParam))
                                        #print ('--> configCategory : '+str(configCategory))
                                        #print ('--> configArea : '+str(configArea))
                                        
                                        b = generate_statistics_baseline_v2()
                                        # create data statistics
                                        b.run_slices(slice_defs, configParam, configCategory, configArea, generalDefs['cpu_count'])
                                        
                                    if statType == "baseStatisticV2":
                                        
                                        if groupbyCategory == 0:
                                            configCategory['h5py_file'] = -1
                                            
                                        b = generate_statistics_basestats_v2()
                                        # create data statistics
                                        b.run(configParam, configCategory, configArea, area, 'avg')
                                        
                                    if statType == "baseStatisticSumV2":
                                        
                                        if groupbyCategory == 0:
                                            configCategory['h5py_file'] = -1
                                            
                                        b = generate_statistics_basestats_v2()
                                        # create data statistics
                                        b.run(configParam, configCategory, configArea, area, 'sum')
                                        

    def check_files(self, statDef, generalDefs, currentScenario):
        #print("check files")
        f_param, f_category, f_area = -1, -1, -1
        
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
            
        if     statDef['idArea'] != "":
            f_area = self.check_areaFile(generalDefs, currentScenario, str(statDef['idArea']), str(statDef['idLevel']))
        else:
            print("missing area id")
            sys.exit()
            
        return f_param, f_category, f_area

    def check_paramFile(self, Defs, DefScenario, idParam, idLevel):
        #print("check parameter file")
        f_param = -1
        decimals = 0
        dataType = None
        config = {}
        
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
                #print('--> parameter file found: ' + file)
            else:
                config['h5py_file'] = -1
                print('--> missing parameter file (h5 or nc): ' + file)
        else:
            config['h5py_file'] = -1
            print('--> parameter path not found: ' + str(path))
            
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
