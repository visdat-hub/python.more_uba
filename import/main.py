            # multiprocessing baseline statistics for netCDF check_files
# calculation count of single values
# statistic values have to be calculated by the function get_statistic_values.py
# use following code for multiprocessing baseline statistics
# python main_baseline.py -p generate_statistics -f /mnt/galfdaten/daten_stb/more_uba/config/processing/statistics/3_1_baseline_2.config
from __future__ import unicode_literals
import sys
sys.path.append('/mnt/galfdaten/Programmierung/tools/more_uba/python/')
from general import general
from module.import_parameter import import_parameter
from module.import_area import import_area
from module.import_p import import_p
from module.generate_statistics import generate_statistics
from module.copy_parameter import copy_parameter
from module.delete_parameter import delete_parameter

if __name__ == "__main__":

    print ("--> starting program to import geodata to stoffbilanz model grid structure...")
    
    g = general()
    
    processingConfig = g.load_config()
    print('processingConfig-->'+str(processingConfig))
    #sys.exit()
    
    process = g.preProcess
    print("--> process: " + process)
    print("--> processingConfig_process: " +  str(processingConfig['process']))
    #sys.exit()
    
    if process == "import_p" and processingConfig['process'] == 'import_p':
        #gridLevelConfig = g.load_gridlevel_config(processingConfig)
        #print('gridLevelConfig-->'+str(gridLevelConfig))
        p = import_p()
        p.doProcessing(processingConfig)
    
    if process == "import_area" and processingConfig['process'] == 'import_area':
        gridLevelConfig = g.load_gridlevel_config(processingConfig)
        #print('gridLevelConfig-->'+str(gridLevelConfig))
        p = import_area()
        p.doProcessing(gridLevelConfig, processingConfig)
    
    if process == "import_parameter" and processingConfig['process'] == 'import_parameter':
        gridLevelConfig = g.load_gridlevel_config(processingConfig)
        #print('gridLevelConfig-->'+str(gridLevelConfig)))
        p = import_parameter()
        p.doProcessing(gridLevelConfig, processingConfig)
        
    if process == "generate_statistics" and processingConfig['process'] == 'generate_statistics':
        p = generate_statistics()
        p.do_statistics(processingConfig)
    
    if process == 'copy_parameter' and processingConfig['process'] == 'copy_parameter':
        print('copy_parameter')
        p = copy_parameter()
        p.do_copy(processingConfig)

    if process == 'delete_parameter' and processingConfig['process'] == 'delete_parameter':
        print('delete_parameter')
        p = delete_parameter()
        p.do_delete(processingConfig)
    
    print("--> preprocessing finished")
