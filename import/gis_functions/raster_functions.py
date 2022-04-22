import sys, os, subprocess

class raster_functions():
    def __init__(self):
        print("class raster_functions")

    def rasterize_shape(self, argDict):
        print("rasterize shapefile")
        print argDict
        if os.path.isfile(argDict['shapefile_path'] + ".shp"):
            print("shapefile to rasterize: " + argDict['shapefile_path'] + ".shp")
            x_min, y_min, x_max, y_max = argDict['model_extent']
            p = subprocess.Popen('/bin/bash', shell=True, stdin=subprocess.PIPE)

            saga_string = 'saga_cmd grid_gridding "Shapes to Grid" ' +\
                '-INPUT='+str(argDict['shapefile_path'])+'.shp ' +\
                '-FIELD='+str(argDict['attribute2raster'])+' '+\
                '-USER_GRID=' + argDict['output_raster_path'] + ' '+\
                '-USER_FIT=0 -MULTIPLE=3 -POLY_TYPE=1 -USER_SIZE=' + str(argDict['pixel_size']) + ' '+\
                '-USER_XMIN=' + str(x_min + argDict['pixel_size']/2) + ' '+\
                '-USER_XMAX=' + str(x_max + argDict['pixel_size']/2) + ' '+\
                '-USER_YMIN=' + str(y_min + argDict['pixel_size']/2) + ' '+\
                '-USER_YMAX=' + str(y_max + argDict['pixel_size']/2) + ' '+\
                '-GRID_TYPE=4'

            print saga_string
            p.communicate(saga_string.encode('utf8'))[0]
            del p
        else:
            sys.exit("cannot find shapefile:" + argDict['shapefile_path'] + ".shp")

    def generate_grid_index(self, argDict):
        print("generate grid index (unique id for each grid cell)")
        print argDict

        p = subprocess.Popen('/bin/bash', shell=True, stdin=subprocess.PIPE)

        saga_string = 'saga_cmd grid_tools 21 ' +\
            '-GRID=' + str(argDict['raster_path']) + '.sgrd ' +\
            '-INDEX=' + str(argDict['raster_path']) + '.sgrd ' +\
            '-ORDER=' + str(argDict['sorting_order'])

        print saga_string
        p.communicate(saga_string.encode('utf8'))[0]
        del p

    def resample_grid(self, argDict):
        print("resample a grid")
        print argDict

        x_min, y_min, x_max, y_max = argDict['model_extent']

        p = subprocess.Popen('/bin/bash', shell=True, stdin=subprocess.PIPE)

        saga_string = 'saga_cmd grid_tools 0 ' +\
            '-INPUT=' + str(argDict['input']) + '.sgrd ' +\
            '-USER_GRID=' + str(argDict['user_grid']) + '.sgrd ' +\
            '-KEEP_TYPE -' +\
            argDict['scale_method'] + '='+str(argDict['resampling_method']) + ' ' +\
            '-USER_FIT=' + str(argDict['user_fit']) + ' ' +\
            '-USER_SIZE=' + str(argDict['pixel_size']) + ' '+\
            '-USER_XMIN=' + str(x_min + argDict['pixel_size']/2) + ' '+\
            '-USER_XMAX=' + str(x_max + argDict['pixel_size']/2) + ' '+\
            '-USER_YMIN=' + str(y_min + argDict['pixel_size']/2) + ' '+\
            '-USER_YMAX=' + str(y_max + argDict['pixel_size']/2)

        print saga_string
        p.communicate(saga_string.encode('utf8'))[0]
        del p
