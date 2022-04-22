import sys, os
import json
sys.path.append('/mnt/galfdaten/Programmierung/tools/more_uba/import/module')
sys.path.append('/mnt/galfdaten/Programmierung/tools/more_uba/import/pg')
from export import export
from raster_functions import raster_functions

class modelgrid_levels():
    def __init__(self):
        print("class modelgrid_levels")
        self.configFile = None
        if len(sys.argv) == 3 :
            if sys.argv[2] :
                self.configFile = sys.argv[2]
                print("config file: " + str(self.configFile))
        if self.configFile == None:
            print("missing argument: config file")
            sys.exit("cancel modelgrid level creation")

    def load_config_file(self):
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

    def get_modelgrid_create_options(self, config):
        print("get modelgrid create options")
        try:
            modelgridConfig = config["modelgrid_options"]
        except Exception as e:
            sys.exit("Error: " +  str(e))
            
        return modelgridConfig

    def get_gridlevel_config(self, modelgridCreateOptions):
        print("get gridlevel configuration")
        project = str(modelgridCreateOptions["project"])
        path = modelgridCreateOptions["dataPath"] + project + "/config/grid_levels/grid_levels.config"
        if os.path.isfile(path):
            json_file = open(path)
            try:
                config = json.load(json_file)
            except Exception as e:
                sys.exit(e)
        else:
            sys.exit("gridlevels config file does not exist")
            
        return config

    def create_modelgrids(self, modelgridCreateOptions, gridlevelConfig):
        print("--> create modelgrids")

        project = modelgridCreateOptions['project']
        modelRegion = modelgridCreateOptions['modelregion']
        print("MODEL REGION DEFS", modelRegion)
        modelgridPath = modelgridCreateOptions['dataPath'] + project + "/modelgrids/"
        outFileContainer = []
        
        # create a modelgrid for each resolution
        for item in gridlevelConfig:
            print("GRID LEVEL DEFS", item)
            levelId = item['levelId']
            fname = "modelgrid_" + str(item['resolution']) + "_" + str(levelId)
            print("FILE NAME", fname)
            # rasterize modelgrid and save as sgrd
            outFile = self.rasterize_data(modelgridPath, fname, modelRegion, item)
            outFileContainer.append(outFile)
            # create unique ids
            if int(modelgridCreateOptions['generate_unique_ids']) == 1:
                if item['resolution'] in modelgridCreateOptions['unique_ids_gridlevels']:
                    outFile = self.generate_unique_ids(modelgridPath, fname)
            # downscale modelgrids
                    if int(modelgridCreateOptions['scale_down_grid_levels']) == 1:
                        outFiles = self.downscale_modelgrid(modelgridPath, fname, item, gridlevelConfig)
                        for i in outFiles:
                            outFileContainer.append(i)
                            
        # export sgrd files to other raster formats
        for file in outFileContainer:
            self.export_raster(file, modelgridCreateOptions)

    def rasterize_data(self, path, fname, modelRegion, config):
        print("--> rasterize data")
        # shapefilepath, attribute2raster, output_raster_path, pixel_size, model_extent, output
        argDict = {
            "output_raster_path" : path + fname,
            "shapefile_path" : modelRegion['path'] + modelRegion['fname'],
            "attribute2raster" : modelRegion['attribute'],
            "pixel_size" : config['resolution'],
            "model_extent" : [config['minx'], config['miny'], config['maxx'], config['maxy']]
        }
        r = raster_functions()
        r.rasterize_shape(argDict)
        return path + fname

    def generate_unique_ids(self, path, fname):
        print("--> generate unique ids for each raster point")
        argDict = {
            "raster_path" : path + fname,
            "sorting_order" : 0
        }
        r = raster_functions()
        r.generate_grid_index(argDict)
        return path + fname

    def downscale_modelgrid(self, path, baseFname, basegridConfig, gridlevelConfig):
        print("--> downscale modelgrid")
        baselevelId = basegridConfig['levelId']
        baseResolution = basegridConfig['resolution']
        outFnames = []
        for item in gridlevelConfig:
            if item['levelId'] > baselevelId:
                levelId = item['levelId']
                outFname = "modelgrid_" + str(baseResolution) + "_" + str(levelId)
                argDict = {
                    "input" : path + baseFname,
                    "user_grid" : path + outFname,
                    "scale_method" : "SCALE_DOWN_METHOD",
                    "resampling_method" : 0,
                    "user_fit" : 0,
                    "pixel_size" : item["resolution"],
                    "model_extent" : [item['minx'], item['miny'], item['maxx'], item['maxy']]
                }
                r = raster_functions()
                r.resample_grid(argDict)
                outFnames.append(path + outFname)
        return outFnames

    def export_raster(self, file, config):
        print("--> export modelgrids (*.sgrd) to other raster formats")
        argDict = {
            "inFile" : file + '.sgrd',
            "outFile" : file,
            "outFormat" : config['grid_format'][0],
            "noData" : config['nodata'],
            "dataType" : 5
        }
        d = export()
        print ('--> argDict : '+str(argDict))
        d.export_grid(argDict)
