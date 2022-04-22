import sys
import subprocess
import glob
sys.path.append('/mnt/galfdaten/Programmierung/tools/more_uba/import/pg')
from pg import db_connector

class delete_parameter():

    def __init__(self):
        print("class delete_parameter")

    def do_delete(self, dataConfig):

        print("--> calculate statistics")

        base_path = dataConfig["general"]["projectDir"]
        optionsDefs = dataConfig["options"]
        db_config = dataConfig["pg_database"]

        pg = db_connector()
        pg.dbConfig(db_config)

        for scenario in optionsDefs['idscenario']:

            pg.dbConnect()
            record,  rowcount = pg.tblSelect( "SELECT idsz, model_scenario_name, model_year, model_project_name  FROM spatial.scenario WHERE idsz = "+str(scenario))
            pg.dbClose()

            #print('-->record : '+str(record))
            #print('-->rowcount : '+str(rowcount))
            currentScenarioList = list(record[0])
            #print('--> currentScenarioList : ' + str(currentScenarioList))

            #idScenario = str(currentScenarioList[0])
            scenarioName = str(currentScenarioList[1])
            modelYear = str(currentScenarioList[2])
            projectName = str(currentScenarioList[3])

            for param in optionsDefs['idparam']:

                for level in optionsDefs['idlevel']:

                    parameter_path = base_path +  projectName + '/parameters/' + scenarioName + '/' + str(param) + '/' + modelYear
                    print('-->parameter_path : '+str(parameter_path))

                    for deleteFile in glob.glob(parameter_path+'/*'):

                        #print('-->deleteFile : '+str(deleteFile))
                        metaInfos = deleteFile.split('.', 1)
                        #print('-->metaInfos : '+str(metaInfos))
                        metaData = metaInfos[0].split('_')
                        #print('-->metaData : '+str(metaData))
                        currLevel = metaData[4]
                        #print('-->currLevel : '+str(currLevel))
                        #print('-->level : '+str(level))

                        if int(currLevel) == int(level):

                            print ('Delete deleteFile from '+str(deleteFile))
                            p = subprocess.Popen('/bin/bash', shell=True, stdin=subprocess.PIPE)

                            # del parameter
                            shell_string = 'rm ' + deleteFile + ';'
                            #print (shell_string)

                            p.communicate(shell_string.encode('utf8'))[0]
                            del p


                            pg.dbConnect()
                            pg.tblDeleteRows('viewer_data.param_area_exists', 'idparam = ' + str(param)  + ' and idsz = ' + str(scenario)  + ' and idlevel = ' + str(level))
                            pg.dbClose()
