# -*- coding: utf-8 -*-
from __future__ import unicode_literals
#import time
import numpy
#import psycopg2 # Postgresql-connection
#import time
#from general.db_connector import db_connector
from general.project_data import ProjectData
#import glob

## This is the main class of the module water_balance
# @date  17.03.2020
#
# @author Stephan Bürger <br>
#        GALF bR <br>
#        01277 Dresden <br>
#        Am Ende 14 <br>
#        http://www.galf-dresden.de <br>
#        info@galf-dresden.de <br>
#
#Die Klasse enthält

class PBudget():

    # Konstruktor
    def __init__(self, configObject):

        self.projectname = configObject['general']['projectname']
        self.scenario = configObject['general']['scenario']
        self.years = configObject['general']['years']
        self.path_data = configObject['general']['path_data']
        self.cellsize = configObject["general"]["cellsize"]
        self.epsg = configObject["general"]["epsg"]
        self.minx = configObject["general"]["minx"]
        self.miny = configObject["general"]["miny"]
        self.maxx = configObject["general"]["maxx"]
        self.maxy = configObject["general"]["maxy"]
        self.levelId = configObject['general']['levelId']
        self.project_data = ProjectData(configObject)
    
    
    def cal_union_schluff(self):
    
        for year in self.years:
            
            scenario_id = self.project_data.get_scenario_id(self.scenario, year)
            print('-->scenario_id ',scenario_id)
            
            __input_data = {}
            __input_data = self.project_data.get_data_p_budget('union_schluff', year, '')
            
            __result = numpy.where(__input_data['sachsen'] > 0, __input_data['sachsen'],  __input_data['deutschland']) 
            
            del __input_data['deutschland']
            del __input_data['sachsen']
            
            targetParameter = 244
            dataType = 'int32' #'int32', 'byte', 'float64', 'float16'
            comma = 2
            targetFileName = str(targetParameter)+'_'+str(scenario_id)+'_'+str(self.levelId)+'.'+str(dataType)+'.'+str(comma)+'.nc'
            self.project_data.set_data(__result, targetParameter, year, targetFileName, True)
        
        return __result
    
    
    def cal_union_ton(self):
    
        for year in self.years:
            
            scenario_id = self.project_data.get_scenario_id(self.scenario, year)
            print('-->scenario_id ',scenario_id)
            
            __input_data = {}
            __input_data = self.project_data.get_data_p_budget('union_ton', year, '')
            
            __result = numpy.where(__input_data['sachsen'] > 0, __input_data['sachsen'],  __input_data['deutschland']) 
            
            del __input_data['deutschland']
            del __input_data['sachsen']
            
            targetParameter = 243
            dataType = 'int32' #'int32', 'byte', 'float64', 'float16'
            comma = 2
            targetFileName = str(targetParameter)+'_'+str(scenario_id)+'_'+str(self.levelId)+'.'+str(dataType)+'.'+str(comma)+'.nc'
            self.project_data.set_data(__result, targetParameter, year, targetFileName, True)
        
        return __result
    
    
    def cal_union_humus(self):
    
        for year in self.years:
            
            scenario_id = self.project_data.get_scenario_id(self.scenario, year)
            print('-->scenario_id ',scenario_id)
            
            __input_data = {}
            __input_data = self.project_data.get_data_p_budget('union_humus', year, '')
            
            __result = numpy.where(__input_data['sachsen'] > 0, __input_data['sachsen'],  __input_data['deutschland']) 
            
            del __input_data['deutschland']
            del __input_data['sachsen']
            
            targetParameter = 336
            dataType = 'int32' #'int32', 'byte', 'float64', 'float16'
            comma = 2
            targetFileName = str(targetParameter)+'_'+str(scenario_id)+'_'+str(self.levelId)+'.'+str(dataType)+'.'+str(comma)+'.nc'
            self.project_data.set_data(__result, targetParameter, year, targetFileName, True)
        
        return __result
    
    
    def cal_sdr(self):
    
        for year in self.years:
            
            scenario_id = self.project_data.get_scenario_id(self.scenario, year)
            print('-->scenario_id',scenario_id)
            
            __input_data = {}
            __input_data = self.project_data.get_data_p_budget('sdr', year, '')
            
            waterConnect = numpy.where(__input_data['water_distance'] > 0, 1, 0)
            
            __result = __input_data['sedimenteintrag'] / __input_data['abag'] * waterConnect
            del __input_data['water_distance']
            del __input_data['sedimenteintrag']
            del __input_data['abag']
            
            targetParameter = 60
            dataType = 'int32' #'int32', 'byte', 'float64', 'float16'
            comma = 4
            targetFileName = str(targetParameter)+'_'+str(scenario_id)+'_'+str(self.levelId)+'.'+str(dataType)+'.'+str(comma)+'.nc'
            self.project_data.set_data(__result, targetParameter, year, targetFileName, True)
        
        return __result
    
    
    def cal_pt_boden(self):
    
        for year in self.years:
            
            scenario_id = self.project_data.get_scenario_id(self.scenario, year)
            print('-->scenario_id',scenario_id)
            
            __input_data = {}
            __input_data = self.project_data.get_data_p_budget('pt_boden', year, '')
            
            __hn = __input_data['landuse']
            del __input_data['landuse']
            
            __result = numpy.where((__hn == 1), (__input_data['pt_boden_1']), numpy.nan)
            print ('pt_boden_1')
            del __input_data['pt_boden_1']
            __result = numpy.where((__hn == 2), (__input_data['pt_boden_2']), __result)
            print ('pt_boden_2')
            del __input_data['pt_boden_2']
            __result = numpy.where((__hn == 3), (__input_data['pt_boden_3']), __result)
            print ('pt_boden_3')
            del __input_data['pt_boden_3']
            __result = numpy.where((__hn == 4), (__input_data['pt_boden_4']), __result)
            print ('pt_boden_4')
            del __input_data['pt_boden_4']
            __result = numpy.where((__hn == 5), (__input_data['pt_boden_5']), __result)
            print ('pt_boden_5')
            del __input_data['pt_boden_5']
            __result = numpy.where((__hn == 6), (__input_data['pt_boden_6']), __result)
            print ('pt_boden_6')
            del __input_data['pt_boden_6']
            __result = numpy.where((__hn == 7), (__input_data['pt_boden_7']), __result)
            print ('pt_boden_7')
            del __input_data['pt_boden_7']
            __result = numpy.where((__hn == 11), (__input_data['pt_boden_11']), __result)
            print ('pt_boden_11')
            del __input_data['pt_boden_11']
            __result = numpy.where((__hn == 13), (__input_data['pt_boden_13']), __result)
            print ('pt_boden_13')
            del __input_data['pt_boden_13']
            del __hn
            
            targetParameter = 9
            dataType = 'int32' #'int32', 'byte', 'float64', 'float16'
            comma = 2
            targetFileName = str(targetParameter)+'_'+str(scenario_id)+'_'+str(self.levelId)+'.'+str(dataType)+'.'+str(comma)+'.nc'
            self.project_data.set_data(__result, targetParameter, year, targetFileName, True)
        
        return __result
    
    
    def cal_fg_boden(self):
    
        for year in self.years:
            
            scenario_id = self.project_data.get_scenario_id(self.scenario, year)
            print('-->scenario_id ',scenario_id)
            
            __input_data = {}
            __input_data = self.project_data.get_data_p_budget('fg_boden', year, '')
            
            __result = __input_data['ton'] + __input_data['schluff'] 
            del __input_data['schluff']
            del __input_data['ton']
            
            __result = numpy.where((__result >  100.0), 100.0, __result)
            
            targetParameter = 245
            dataType = 'int32' #'int32', 'byte', 'float64', 'float16'
            comma = 2
            targetFileName = str(targetParameter)+'_'+str(scenario_id)+'_'+str(self.levelId)+'.'+str(dataType)+'.'+str(comma)+'.nc'
            self.project_data.set_data(__result, targetParameter, year, targetFileName, True)
        
        return __result
    
    
    def cal_enr_fein(self):
    
        for year in self.years:
            
            scenario_id = self.project_data.get_scenario_id(self.scenario, year)
            print('-->scenario_id ',scenario_id)
            
            __input_data = {}
            __input_data = self.project_data.get_data_p_budget('enr_fein', year, '')
            
            __fg_boden = __input_data['fg_boden']/100
            del __input_data['fg_boden']
            
            __result = numpy.where((__input_data['sdr'] < __fg_boden), \
                                   (1/__fg_boden),  (1/__input_data['sdr']))
            
            del __fg_boden
            del __input_data['sdr']
            
            targetParameter = 246
            dataType = 'int32' #'int32', 'byte', 'float64', 'float16'
            comma = 4
            targetFileName = str(targetParameter)+'_'+str(scenario_id)+'_'+str(self.levelId)+'.'+str(dataType)+'.'+str(comma)+'.nc'
            self.project_data.set_data(__result, targetParameter, year, targetFileName, True)
        
        return __result
    
    
    def cal_c_sed(self):
    
        for year in self.years:
            
            scenario_id = self.project_data.get_scenario_id(self.scenario, year)
            print('-->scenario_id ',scenario_id)
            
            __input_data = {}
            __input_data = self.project_data.get_data_p_budget('c_sed', year, '')
            
            __result = __input_data['enr_fein'] * __input_data['pt_boden']
            del __input_data['enr_fein']
            del __input_data['pt_boden']
            
            targetParameter = 72
            dataType = 'int32' #'int32', 'byte', 'float64', 'float16'
            comma = 2
            targetFileName = str(targetParameter)+'_'+str(scenario_id)+'_'+str(self.levelId)+'.'+str(dataType)+'.'+str(comma)+'.nc'
            self.project_data.set_data(__result, targetParameter, year, targetFileName, True)
        
        return __result
    
    
    def cal_ppart(self):
    
        for year in self.years:
            
            scenario_id = self.project_data.get_scenario_id(self.scenario, year)
            print('-->scenario_id ',scenario_id)
            
            __input_data = {}
            __input_data = self.project_data.get_data_p_budget('ppart', year, '')
            
            __hn = __input_data['landuse']
            
            __result = __input_data['sediment'] * __input_data['c_sed'] /1000
            
            # offene Flaechen
            __result = numpy.where((__hn == 11), __input_data['p_eros_off'], __result)
            
            
            del __input_data['c_sed']
            del __input_data['sediment']
            del __input_data['p_eros_off']
            
            targetParameter = 74
            dataType = 'int32' #'int32', 'byte', 'float64', 'float16'
            comma = 4
            targetFileName = str(targetParameter)+'_'+str(scenario_id)+'_'+str(self.levelId)+'.'+str(dataType)+'.'+str(comma)+'.nc'
            self.project_data.set_data(__result, targetParameter, year, targetFileName, True)
        
        return __result
    
    def cal_p_diff(self):
    
        for year in self.years:
            
            scenario_id = self.project_data.get_scenario_id(self.scenario, year)
            print('-->scenario_id ',scenario_id)
            
            __input_data = {}
            __input_data = self.project_data.get_data_p_budget('p_diff', year, '')
            
            print('1')
            __result = __input_data['p_ag']
            del __input_data['p_ag']
            print('2')
            __result = __result + __input_data['p_ao']
            del __input_data['p_ao']
            print('3')
            __result = __result + __input_data['p_draen']
            del __input_data['p_draen']
            print('4')
            __result = __result + __input_data['p_atm_gew']
            del __input_data['p_atm_gew']
            print('5')
            
            __p_part = numpy.where((numpy.isnan(__input_data['p_part'])), 0.0, __input_data['p_part'])
            del __input_data['p_part']
            print('6')
            
            __result = numpy.where((numpy.isnan(__input_data['landuse'])), numpy.nan, __p_part)
            del __input_data['landuse']
            print('7')
            
            __result = __result + __p_part
            
            targetParameter = 75
            dataType = 'int32' #'int32', 'byte', 'float64', 'float16'
            comma = 4
            targetFileName = str(targetParameter)+'_'+str(scenario_id)+'_'+str(self.levelId)+'.'+str(dataType)+'.'+str(comma)+'.nc'
            self.project_data.set_data(__result, targetParameter, year, targetFileName, True)
        
        return __result
    
    def cal_p_urban(self):
    
        for year in self.years:
            
            scenario_id = self.project_data.get_scenario_id(self.scenario, year)
            print('-->scenario_id ',scenario_id)
            
            __input_data = {}
            __input_data = self.project_data.get_data_p_budget('p_urban', year, '')
            
            __result = __input_data['p_ka_50']+__input_data['p_ka_50_2000'] \
                +__input_data['p_mka']+__input_data['p_tka']
            
            del __input_data['p_ka_50']
            del __input_data['p_ka_50_2000']
            del __input_data['p_mka']
            del __input_data['p_tka']
            
            targetParameter = 76
            dataType = 'int32' #'int32', 'byte', 'float64', 'float16'
            comma = 4
            targetFileName = str(targetParameter)+'_'+str(scenario_id)+'_'+str(self.levelId)+'.'+str(dataType)+'.'+str(comma)+'.nc'
            self.project_data.set_data(__result, targetParameter, year, targetFileName, True)
        
        return __result
    
    
    def cal_p_sied_ges(self):
        
        for year in self.years:

            scenario_id = self.project_data.get_scenario_id(self.scenario, year)
            print(scenario_id)
            
            __input_data = {}
            __input_data = self.project_data.get_data_p_budget('p_sied_ges', year, '')
            
            __p_urban = numpy.where((numpy.isnan(__input_data['p_urban'])), 0.0, __input_data['p_urban']/100)
            del __input_data['p_urban']
            __p_ka = numpy.where((numpy.isnan(__input_data['p_ka'])), 0.0, __input_data['p_ka'])
            del __input_data['p_ka']
            __result = __p_urban + __p_ka
            
            __result = numpy.where((numpy.isnan(__input_data['landuse'])), numpy.nan, __result)
            del __input_data['landuse']
            
            targetParameter = 77
            dataType = 'int64' #'int32', 'byte', 'float64', 'float16'
            comma = 6 #64, 16, 2, 0
            targetFileName = str(targetParameter)+'_'+str(scenario_id)+'_'+str(self.levelId)+'.'+str(dataType)+'.'+str(comma)+'.nc'
            self.project_data.set_data(__result, targetParameter, year, targetFileName, True)
        
        return __result
    
    
    def cal_pges(self):
        
        for year in self.years:

            scenario_id = self.project_data.get_scenario_id(self.scenario, year)
            print(scenario_id)
            
            __input_data = {}
            __input_data = self.project_data.get_data_p_budget('pges', year, '')
            
            __p_diff = numpy.where((numpy.isnan(__input_data['p_diff'])), 0.0, __input_data['p_diff']/100)
            del __input_data['p_diff']
            __p_urban = numpy.where((numpy.isnan(__input_data['p_urban'])), 0.0, __input_data['p_urban']/100)
            del __input_data['p_urban']
            __p_ka = numpy.where((numpy.isnan(__input_data['p_ka'])), 0.0, __input_data['p_ka'])
            del __input_data['p_ka']
            __result = __p_diff + __p_urban + __p_ka
            
            __result = numpy.where((numpy.isnan(__input_data['landuse'])), numpy.nan, __result)
            del __input_data['landuse']
            
            targetParameter = 78
            dataType = 'int64' #'int32', 'byte', 'float64', 'float16'
            comma = 6 #64, 16, 2, 0
            targetFileName = str(targetParameter)+'_'+str(scenario_id)+'_'+str(self.levelId)+'.'+str(dataType)+'.'+str(comma)+'.nc'
            self.project_data.set_data(__result, targetParameter, year, targetFileName, True)
        
        return __result
    
    
    def cal_ppart_test(self):
    
        for year in self.years:

            scenario_id = self.project_data.get_scenario_id(self.scenario, year)
            print('-->scenario_id',scenario_id)
            
            __input_data = {}
            __input_data = self.project_data.get_data_p_budget('ppart_test', year, '')
            
            __hn = __input_data['landuse']
            
            __result = numpy.where((__hn == 1), (__input_data['ppart_1410']), numpy.nan)
            print ('ppart_1410')
            del __input_data['ppart_1410']
            __result = numpy.where((__hn == 2), (__input_data['ppart_1411']), __result)
            print ('ppart_1411')
            del __input_data['ppart_1411']
            __result = numpy.where((__hn == 3), (__input_data['ppart_1412']), __result)
            print ('ppart_1412')
            del __input_data['ppart_1412']
            __result = numpy.where((__hn == 4), (__input_data['ppart_1413']), __result)
            print ('ppart_1413')
            del __input_data['ppart_1413']
            __result = numpy.where((__hn == 5), (__input_data['ppart_1414']), __result)
            print ('ppart_1414')
            del __input_data['ppart_1414']
            __result = numpy.where((__hn == 6), (__input_data['ppart_1415']), __result)
            print ('ppart_1415')
            del __input_data['ppart_1415']
            __result = numpy.where((__hn == 7), (__input_data['ppart_1416']), __result)
            print ('ppart_1416')
            del __input_data['ppart_1416']
            __result = numpy.where((__hn == 8), (__input_data['ppart_1417']), __result)
            print ('ppart_1417')
            del __input_data['ppart_1417']
            __result = numpy.where((__hn == 9), (__input_data['ppart_1418']), __result)
            print ('ppart_1418')
            del __input_data['ppart_1418']
            del __hn
            
            
            targetParameter = 74
            dataType = 'int32' #'int32', 'byte', 'float64', 'float16'
            comma = 2
            targetFileName = str(targetParameter)+'_'+str(scenario_id)+'_'+str(self.levelId)+'.'+str(dataType)+'.'+str(comma)+'.nc'
            self.project_data.set_data(__result, targetParameter, year, targetFileName, True)
        
        return __result
        