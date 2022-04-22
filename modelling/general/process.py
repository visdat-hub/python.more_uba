# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import shutil
import os
from general.project_data import ProjectData
from p_budget.p_budget import PBudget

class ProcessStb():

    ## Konstruktor.
    def __init__(self, configObject):

        self.projectname = configObject['general']['projectname']
        self.scenario = configObject['general']['scenario']
        self.years = configObject['general']['years']
        self.path_data = configObject['general']['path_data']
        self.epsg = configObject["general"]["epsg"]

        #self.pd = ProjectData(self.projectname, self.path_tmp, self.path_input, self.path_preparation, configObject)

    def cleanDataFolder(self):

        print ("Clean Folders ...")

        shutil.rmtree(self.path_result)
        os.makedirs(self.path_result)
        shutil.rmtree(self.path_tmp)
        os.makedirs(self.path_tmp)
        shutil.rmtree(self.path_preparation)
        os.makedirs(self.path_preparation)

        return 'ok'

    def baseProcessing(self, configObject):

        ##################################################
        ################# Projektdata ####################
        ##################################################
        pd = ProjectData(configObject)
        ##################################################
        return pd

class ProcessStbP():

    ## Konstruktor.
    def __init__(self, configObject):

        self.projectname = configObject['general']['projectname']
        self.scenario = configObject['general']['scenario']
        self.years = configObject['general']['years']
        self.path_data = configObject['general']['path_data']
        self.epsg = configObject["general"]["epsg"]

    def baseProcessing(self, configObject):

        ###############################################
        ################# P-Bilanz ####################
        ###############################################
        pb = PBudget(configObject)
        ##################################################
        
        #244
        #pb.cal_union_schluff()
        
        #2431
        #pb.cal_union_ton()
        
        #336
        #pb.cal_union_humus()
        
        # 21
        #pb.cal_pt_boden()
        
        # 60
        #pb.cal_sdr()
        
        # 245
        #pb.cal_fg_boden()
        
        # 246
        #pb.cal_enr_fein()
        
        # 72
        #pb.cal_c_sed()
        
        # 74
        #pb.cal_ppart()
        
        # 75
        #pb.cal_p_diff()
        
        # 76
        #pb.cal_p_urban()
        
        # 77
        #pb.cal_p_sied_ges()
        
        # 78
        pb.cal_pges()