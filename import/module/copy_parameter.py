import os, sys
import shutil
import subprocess
import glob
sys.path.append('/mnt/galfdaten/Programmierung/tools/more_uba/import/pg')

from pg import db_connector

class copy_parameter():

    def __init__(self):
        print("class copy_parameter")

    def do_copy(self, dataConfig):

        print("--> copy parameter")
        base_path = dataConfig["general"]["projectDir"] + dataConfig["general"]["project"] + '/' + 'parameters'

        pg = db_connector()
        pg.dbConfig(dataConfig["pg_database"])
        pg.dbConnect()

        sqlTxt = "SELECT get_id_spatial_scenario AS id FROM spatial.get_id_spatial_scenario('"+str(dataConfig['general']['project'])+"', '"+str(dataConfig['options']['source_scenario_name'])+"', '"+str(dataConfig['options']['source_year'])+"')"
        print('--> sqlTxt_source : ' + sqlTxt)
        retVal, row_count = pg.tblSelect(sqlTxt)
        source_scenario_id = retVal[0][0]
        pg.dbClose()

        for parameter_id in dataConfig["options"]["idparam"]:

            parameter_path = base_path + '/' + dataConfig["options"]["source_scenario_name"] + '/' + str(parameter_id) +'/'+dataConfig["options"]["source_year"]

            for target_year in dataConfig["options"]["target_years"]:

                if dataConfig["options"]["source_year"] != target_year or dataConfig["options"]["source_scenario_name"] != dataConfig["options"]["target_scenario_name"]:

                    target_parameter_path = base_path + '/' + dataConfig["options"]["target_scenario_name"] + '/' + str(parameter_id) +'/'+ str(target_year) +'/'

                    if os.access(target_parameter_path, os.F_OK) is True:
                        shutil.rmtree(target_parameter_path)
                        os.makedirs(target_parameter_path)
                    else:
                        os.makedirs(target_parameter_path)

                    pg.dbConnect()
                    sqlTxt = "SELECT get_id_spatial_scenario AS id FROM spatial.get_id_spatial_scenario('"+str(dataConfig['general']['project'])+"', '"+str(dataConfig['options']['target_scenario_name'])+"', '"+str(target_year)+"')"
                    #print('--> sqlTxt_target : ' + sqlTxt)
                    retVal, row_count = pg.tblSelect(sqlTxt)
                    target_scenario_id = retVal[0][0]
                    #print('--> target_scenario_id : ' + str(target_scenario_id))
                    pg.dbClose()

                    levelArray = []
                    #print(parameter_path)
                    #print(glob.glob(parameter_path+'/*'))

                    for inputFile in glob.glob(parameter_path+'/*'):

                        inputFileName = inputFile.replace(parameter_path+'/', '')
                        metaInfos = inputFileName.split('.', 1)
                        metaData = metaInfos[0].split('_')

                        levelArray.append(int(metaData[2]))

                        outputFileName = inputFileName.replace('_'+str(source_scenario_id)+'_', '_'+str(target_scenario_id)+'_')

                        print ('Change Inputfilename from '+str(inputFileName)+' to '+str(outputFileName))
                        p = subprocess.Popen('/bin/bash', shell=True, stdin=subprocess.PIPE)

                        if dataConfig["options"]["symbolicLink"] == 0:
                            # copy parameter
                            shell_string = 'cp ' + inputFile + ' ' + target_parameter_path + outputFileName+';'
                        else:
                            # create link
                            shell_string = 'ln -sr ' + inputFile + ' ' + target_parameter_path + outputFileName+';'

                        #print (shell_string)

                        p.communicate(shell_string.encode('utf8'))[0]
                        del p

                    #print('-->  : ' + str(set(levelArray)))

                    #vorhandene Level durchgehen
                    for levelId in set(levelArray):
                        print (levelId)

                        #in Datenbank anlegen zur Visualisierung im viewer
                        pg.dbConnect()
                        sqlTxt = "SELECT * from general.sbvf_insert_param_area_exists("+str(levelId)+","+str(target_scenario_id)+","+str(parameter_id)+",'"+str(dataConfig["general"]["projectDir"])+"');"
                        #print(sqlTxt)
                        retVal, row_count = pg.tblSelect(sqlTxt)
                        pg.dbClose()


                else:
                    print ('Aufpassen!!!!!!!! source_year in target_years enthalten')
