import sys, os
import json
import h5py
import numpy as np
sys.path.append('/mnt/galfdaten/Programmierung/tools/more_uba/data_exchange')
from export import export

class gridlevel_data():
    def __init__(self):
        print("class gridlevel_data")
        self.configFile = None
        if len(sys.argv) == 3 :
            if sys.argv[2] :
                self.configFile = sys.argv[2]
                print("config file: " + str(self.configFile))
        if self.configFile == None:
            print("missing argument: config file")
            sys.exit("cancel gridlevel data generation")

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

    def get_gridlevel_create_options(self, config):
        print("get grid level create options")
        try:
            gridLevelConfig = config["create_gridlevel_options"]
        except Exception as e:
            sys.exit("Error: " +  str(e))
        return gridLevelConfig

    def get_extent(self, create_options):
        print("--> get extent from data source")
        createMethod = create_options["create_method"]
        print("create method : " + createMethod)
        if createMethod == 'from_grid':
            height, width, minx, miny, maxx, maxy = self.get_extent_from_grid(create_options)
        if createMethod == 'user_defined':
            height, width, minx, miny, maxx, maxy = self.get_extent_from_json(create_options)

        print (height, width, minx, miny, maxx, maxy)
        return height, width, minx, miny, maxx, maxy

    def get_extent_from_grid(self, create_options):
        print("get extent from grid")
        grid_path = create_options['from_grid']['path']
        resolution = float(create_options['from_grid']['resolution'])
        print("grid path: " + str(grid_path))
        if grid_path.split(".")[-1] == 'nc':
            print("grid format: netcdf")
            f = h5py.File(grid_path, 'r')
            grid_x = f['x'][:]
            grid_y = f['y'][:]
            height = grid_y.size
            width = grid_x.size
            minx = np.nanmin(grid_x) - resolution/2
            miny = np.nanmin(grid_y) - resolution/2
            maxx = np.nanmax(grid_x) + resolution/2
            maxy = np.nanmax(grid_y) + resolution/2

        return height, width, minx, miny, maxx, maxy

    def get_extent_from_json(self, create_options):
        print("get extent from json")
        resolution = float(create_options['user_defined']['resolution'])
        minx = float(create_options['user_defined']['minx'])
        miny = float(create_options['user_defined']['miny'])
        maxx = float(create_options['user_defined']['maxx'])
        maxy = float(create_options['user_defined']['maxy'])
        height = (maxy - miny) / resolution
        width = (maxx - minx) / resolution

        return height, width, minx, miny, maxx, maxy

    def calculate_max_extent(self, height, width, minx, miny, maxx, maxy, gridLevelCreateOptions):
        print("--> calculate extent for min resolution (lowest resolution, highest pixel size)")
        min_resolution = np.max(gridLevelCreateOptions['grid_levels'])
        if gridLevelCreateOptions['create_method'] == "from_grid":
            base_grid_resolution = float(gridLevelCreateOptions['from_grid']['resolution'])
        if gridLevelCreateOptions['create_method'] == "user_defined":
            base_grid_resolution = float(gridLevelCreateOptions['user_defined']['resolution'])
        print("max pixel size: " + str(min_resolution))
        max_maxx = minx + np.round((width * base_grid_resolution) / min_resolution, 0) * min_resolution
        if max_maxx < maxx:
            max_maxx = max_maxx + min_resolution
        max_maxy = miny + np.round((height * base_grid_resolution) / min_resolution, 0) * min_resolution
        if max_maxy < maxy:
            max_maxy = max_maxy + min_resolution

        print (minx, miny, max_maxx, max_maxy)
        return minx, miny, max_maxx, max_maxy

    def calculate_gridlevel_data(self, minx, miny, maxx, maxy, gridLevelCreateOptions):
        print("--> calculate grid level data")
        gridLevelData = []
        gridLevels = np.array(gridLevelCreateOptions['grid_levels'])
        gridLevels = -np.sort(-gridLevels) # reverse sorting of grid levels
        for r in gridLevels:
            print("grid level : " + str(r) + ' meters')
            height = int((maxy - miny) / r)
            width = int((maxx - minx) / r)
            geotransform = str(minx + r/2) + ', ' + str(r) \
                + ', 0, ' + str(miny + r/2) + ', 0, ' + str(r)
            epsg = str(gridLevelCreateOptions['epsg_code'])
            #ds = np.empty([height, width])
            #band_x = np.arange(minx + r/2, maxx, r)
            #band_y = np.arange(miny + r/2, maxy, r)
            gridLevelData.append([r, height, width, minx, miny, maxx, maxy, geotransform, epsg])
        print (gridLevelData)
        return gridLevelData

    def save_gridlevel_data(self, gridLevelData, gridLevelCreateOptions):
        print("--> save gridlevel data to json file")
        project = str(gridLevelCreateOptions["project"])
        path = gridLevelCreateOptions['dataPath'] + project + "/config/"
        epsg_code = gridLevelCreateOptions['epsg_code']
        fname = "grid_levels/grid_levels.config"
        data = []
        levelId = 0
        for item in gridLevelData:
            levelId = levelId + 1
            l = {
                "levelId" : levelId,
                "resolution" : item[0],
                "height" : item[1],
                "width" : item[2],
                "minx" : item[3],
                "miny" : item[4],
                "maxx" : item[5],
                "maxy" : item[6],
                "geotransform" : item[7],
                "epsg" : epsg_code
            }
            data.append(l)

        d = export()
        d.json(data, path, fname)
