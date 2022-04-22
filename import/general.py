import sys, os
import json

class general():
    
    def __init__(self):
        print("class general")
        self.configFile = None
        self.preProcess = None
        i = 1
        while i < len(sys.argv):
            arg = sys.argv[i]
            if arg == '-f':
                i = i + 1
                self.configFile = sys.argv[i]
                print("config file: " + str(self.configFile))
            elif arg == '-p':
                i = i + 1
                self.preProcess = sys.argv[i]
                print("preprocessing activity: " + str(self.preProcess))
            i = i + 1

        if self.configFile == None:
            print("missing argument: config file")
            sys.exit("cancel preprocessing")
        if self.preProcess == None:
            print("missing argument: process")
            sys.exit("cancel preprocessing")

    def load_config(self):
        print("load config file")
        config = None
        path = self.configFile
        if os.path.isfile(path):
            json_file = open(path)
            try:
                config = json.load(json_file)
            except Exception as e:
                sys.exit(e)
        else:
            sys.exit("config file does not exist")
        return config

    def load_gridlevel_config(self, config):
        print("load grid level configuration")
        gridLevelConfig = None
        #print config
        path = config["general"]["gridLevelConfig"]
        if os.path.isfile(path):
            json_file = open(path)
            try:
                gridLevelConfig = json.load(json_file)
            except Exception as e:
                sys.exit(e)
        else:
            print("gridlevel config file does not exist")
            sys.exit("!! please create gridlevel config file first")
        return gridLevelConfig
