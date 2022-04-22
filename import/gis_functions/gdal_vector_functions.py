import subprocess

class gdal_vector_functions():

    def __init__(self):
        print("class gdal_vector_functions")

    #ogr2ogr [--help-general] [-skipfailures] [-append] [-update]
    #[-select field_list] [-where restricted_where|\@filename]
    #[-progress] [-sql <sql statement>|\@filename] [-dialect dialect]
    #[-preserve_fid] [-fid FID] [-limit nb_features]
    #[-spat xmin ymin xmax ymax] [-spat_srs srs_def] [-geomfield field]
    #[-a_srs srs_def] [-t_srs srs_def] [-s_srs srs_def] [-ct string]
    #[-f format_name] [-overwrite] [[-dsco NAME=VALUE] ...]
    #dst_datasource_name src_datasource_name
    def reproject_vector_data(self, argDict):
        print("reproject data")
        print (argDict)
        t_srs = "-t_srs EPSG:" + argDict['t_srs']
        a_srs = "-a_srs EPSG:" + argDict['s_srs']
        dst_file = argDict['dst_file']
        src_file = argDict['src_file']

        gdal_string = "ogr2ogr " + t_srs + ' ' + a_srs + ' ' + dst_file + ' ' + src_file
        print (gdal_string)
        p = subprocess.Popen('/bin/bash', shell=True, stdin=subprocess.PIPE)
        p.communicate(gdal_string.encode('utf8'))[0]
        del p
