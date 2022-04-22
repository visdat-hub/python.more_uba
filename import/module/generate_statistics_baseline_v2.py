# baseline do_statistics
# count of single values grouped by area and landuse
from __future__ import unicode_literals
import sys, os
#import multiprocessing as mp
import h5py
import numpy as np
import pandas as pd
from module.statistics_save import statistics_save

"""
    Managing baseline process. The baseline process calculates the counts of uniques of a parameter
    and save data to hdf5 statistic file.

    Parameters
    ----------
    config: json
        baseline configuration
    """

class generate_statistics_baseline_v2():

    def __init__(self):

        global results2
        global results_area
        results2 = []
        results_area = []

        print("class statistic_baseline")


    # multiprocessing function for grouping and summarizing area, category and counts of slices
    def mp_summarize_slices(self, df, idArea, groupByCategory):
        #if groupByCategory == 1 :
        #    df_stat = df.groupby(['area','category','value'])['count'].agg(['sum']).reset_index()
        #if groupByCategory == 0 :
        #    df_stat = df.groupby(['area','value'])['count'].agg(['sum']).reset_index()
        return idArea, groupByCategory, df


    # write data slices defined by step size mp_step
    def writing_slices2h5(self, key, config, idLevel, mp_step):

        print("--> writing data slices to h5 files")
        #print('--> config : '+str(config))

        fname = config['fname']
        path = config['fpath']
        dataType = config['dtype']
        decimals = config['decimals']
        id = config['id']

        f = config['h5py_file']
        # read Band1
        band1 = f['Band1']
        noData = band1.attrs['_FillValue'][0]
        # read slices
        len_x = len(f['x'][:])
        len_y = len(f['y'][:])

        #print ("len_x " + str(len_x))
        #print ("len_y " + str(len_y))
        #print(key)


        # create path for slice files
        slicePath = path + 'slices/'
        if not os.path.exists(slicePath):
            os.mkdir(slicePath)
        slicePath = path + 'slices/' + str(idLevel) + '/'
        if not os.path.exists(slicePath):
            os.mkdir(slicePath)

        z = 0 # an unique identifier

        i, i2, zi = 0, 0, 0

        while i2 < len_x:

            if zi == 0:
                step_i = 0
                step_i2 = mp_step

            if zi == 1:
                step_i = mp_step + 1
                step_i2 = mp_step

            if zi > 1:
                step_i = mp_step
                step_i2 = mp_step

            i = i + step_i
            i2 = i2 + step_i2

            zi  = zi + 1

            if i2 >= len_x:
                i2 = len_x

            j, j2, zj = 0, 0, 0
            while j2 < len_y:

                if zj == 0:
                    step_j = 0
                    step_j2 = mp_step

                if zj == 1:
                    step_j = mp_step + 1
                    step_j2 = mp_step

                if zj > 1:
                    step_j = mp_step
                    step_j2 = mp_step

                j = j + step_j
                j2 = j2 + step_j2

                zj  = zj + 1

                if j2 >= len_y:
                    j2 = len_y
                #print ('i i2 j j2', i, i2, j, j2)

                if zj == 0:
                    zj = zj + 1

                z = z + 1

                # create new h5 file to store slices
                fpath_slice = slicePath + fname + '.' + str(dataType) + '.' + str(decimals) + '.slice_' + str(z) + '.h5'
                #print(fpath_slice)
                if os.path.exists(fpath_slice):
                    # remove if exists and write a new file OR continue
                    #os.remove(fpath_slice)
                    continue
                else:
                    print("... creating file : " + fpath_slice)

                f_slice = h5py.File(fpath_slice, 'a')
                np_param = np.array(band1[j:j2,i:i2]).astype(float)
                dset = f_slice.create_dataset("Band1", data = np_param)#, compression="gzip", compression_opts=9)
                dset.attrs['slice'] = [j,j2,i,i2]
                dset.attrs['_FillValue'] = noData
                dset.attrs['sliceId'] = z

                if key == 'param' or key == 'category':
                    dset.attrs['idParam'] = id
                if key == 'area':
                    dset.attrs['idArea'] = id
                f_slice.close()
        f.close()

        #sys.exit()

    # prepare multiprocessing data loading and calculation
    def mp_load_data(self, slice_defs, cpu_count):
        print("--> load data")
        #pool = mp.Pool(int(cpu_count))
        print("--> start multiprocessing")

        z = 0
        for item in slice_defs:

            print('process : ' + str(z) + ' of ' + str(len(slice_defs)))
            #print('item-->'+str(item))
            z = z + 1
            area = item['area']
            parameter = item['parameter']
            category = item['category']

            h5py_file_category = item['configCategory']['h5py_file']
            h5py_file_area = item['configArea']['h5py_file']
            h5py_file_param = item['configParam']['h5py_file']

            if os.path.isfile(parameter['fpath']):
                h5py_file_param = h5py.File(parameter['fpath'], 'r')
                print('--> parameter file found: ' + parameter['fpath'])
            else :
                print('--> parameter file not found: ' + parameter['fpath'])

            if os.path.isfile(area['fpath']):
                h5py_file_area = h5py.File(area['fpath'], 'r')
                print('--> area file found: ' + area['fpath'])
            else :
                print('--> area file not found: ' + area['fpath'])

            if os.path.isfile(category['fpath']):
                h5py_file_category = h5py.File(category['fpath'], 'r')
                print('--> category file found: ' + category['fpath'])
            else :
                print('--> category file not found: ' + category['fpath'])

            #print('configCategory_hd5--> '+ str(h5py_file_category))
            #print('configArea_hd5--> '+ str(h5py_file_area))
            #print('configParam_hd5--> '+str(h5py_file_param))

            results_area.append(self.ac_load_data(h5py_file_category, h5py_file_area, h5py_file_param, area, parameter, category))
            #pool.apply_async(self.ac_load_data, args=(h5py_file_category, h5py_file_area, h5py_file_param, area, parameter, category), callback=self.collect_results_area)

        #pool.close()
        #pool.join()

    # function for asynchronous multiprocessing
    def ac_load_data(self, h5py_file_category, h5py_file_area, h5py_file_param, area, parameter, category):

        idArea = area['idArea']
        groupByCategory = int(area['groupbyCategory'])
        slice = area['slice']
        #print (area)
        #print (parameter)
        #print (category)
        print ('idArea : ' + str(idArea) + ' | slice : ' + str(slice) + ' | groupByCategory : ' + str(groupByCategory))

        ds = h5py_file_area['Band1']
        #print (ds)
        slice_area = ds[:]
        #print (slice_area)
        fillValue_area = area['fillValue']
        #print ('--> fillValue_area : ' + str(fillValue_area))
        # get parameter slice
        ds = h5py_file_param['Band1']
        slice_param = ds[:]
        #print (slice_param)
        fillValue_param = parameter['fillValue']
        #print ('--> fillValue_param : ' + str(fillValue_param))
        # get category slice
        ds = h5py_file_category['Band1']
        slice_category = ds[:]
        #print (slice_category)
        fillValue_category = category['fillValue']
        #print ('--> fillValue_category : ' + str(fillValue_category))

        nodata = [fillValue_param, fillValue_area, fillValue_category]
        print('--> nodata[param, area, category] : ' + str(nodata))
        data = [slice_param, slice_area, slice_category]
        #print ('--> sizeof(data) : ' + str(sys.getsizeof(data)))
        #print ('--> data : ' + str(data))

        r = self.run_baseline(data, nodata, groupByCategory, idArea)
        print ('sizeof(results_area) ' + str(sys.getsizeof(results_area)))
        data, ds = None, None
        slice_param, slice_area, slice_category = None, None, None
        return r

    # statistic function for count values
    def run_baseline(self, data, nodata, groupByCategory, idArea):

        print("_run_baseline()")

        np_param = data[0]
        np_area = data[1]

        #print('idArea--> '+ str(idArea))
        #print('nodata--> '+ str(nodata))
        #print('np_param--> '+ str(np_param))
        #print('np_area--> '+ str(np_area))
        #print('groupByCategory--> '+ str(groupByCategory))

        if groupByCategory == 1:
            np_category = data[2]
            #np_category[np_category == nodata[2]] = np.nan
            #np.where(np_category == nodata[2], np.nan, np_category)

        if groupByCategory == 0:
            df = pd.DataFrame({'area' : np_area.flatten(), 'value' : np_param.flatten()})
            #print (df['value'].size)
            df = df[pd.notnull(df['area'])]
            #print (df['value'].size)
            df = df[pd.notnull(df['value'])]
            #print (df['value'].size)

            df = df[df.value != nodata[0]]
            df = df[df.area != nodata[1]]

            df_stat_0 = df.groupby(['area','value'])['value'].agg(['count']).reset_index()
            df_stat = df_stat_0.groupby(['area','value'])['count'].agg(['sum']).reset_index()
            #print('--------------------------> df_stat start :' +str(df_stat))
            #print('--------------------------> df_stat end :')
            #sys.exit()

        if groupByCategory == 1:
            df = pd.DataFrame({'area' : np_area.flatten(), 'category' : np_category.flatten(), 'value' : np_param.flatten()})
            df = df[pd.notnull(df['area'])]
            df = df[pd.notnull(df['category'])]
            df = df[pd.notnull(df['value'])]

            df = df[df.value != nodata[0]]
            df = df[df.area != nodata[1]]
            df = df[df.category != nodata[2]]

            df_stat_0 = df.groupby(['area','category','value'])['value'].agg(['count']).reset_index()
            df_stat = df_stat_0.groupby(['area','category','value'])['count'].agg(['sum']).reset_index()
        
        #print('df_stat ->',df_stat.shape)
        #print('df_stat ->',df_stat)
        np_param, np_area, np_category = None, None, None
        df_stat_0, df = None, None

        return {'slice_stat' : df_stat, 'idArea' : idArea, 'groupByCategory' : groupByCategory}

    # callback functions for collecting multiprocessing results of _mp_load_data() --> _run_baseline() in mp_load_data()
    def collect_results_area(self, result):
        results_area.append(result)

    # callback functions for collecting multiprocessing results of mp_summarize_slices() in do_statistics_slices()
    def collect_results2(self, result2):
        results2.append(result2)

    # do statistics for data slices
    def run_slices(self, slice_defs, configParam, configCategory, configArea, cpu_count):

        idCategory = configCategory['idCategory']
        idArea = configArea['idArea']

        #print ('--> slice_defs : ' + str(slice_defs))
        #######################################################################
        # do multiprocessing slice statistics
        # get value count for each data slice
        # result container: results_area
        #######################################################################
        print("----------------------------------------------------------------------")
        self.mp_load_data(slice_defs, cpu_count)
        #######################################################################
        # summarizing value counts group by value for all data slices: prepare
        #######################################################################
        
        
        print("----------------------------------------------------------------------")
        print("--> summarizing slices for idArea " + str(idArea))
        df_list = []
        # gathering data for multiprocessing
        #for area in generalDefs['areas']:
        df_category = pd.DataFrame(columns=['area','category','value','sum'])
        df_area = pd.DataFrame(columns=['area','value','sum'])
        print("prepare multiprocessing dataframes")
        #print ('results_area-->'+str(results_area))

        for item in results_area:
            if item['groupByCategory'] == 0 and int(item['idArea']) == int(idArea) :
                df_area = df_area.append(item['slice_stat'], ignore_index=True)
                df_list.append({'idArea' : idArea, 'groupByCategory' : 0, 'df' : df_area})
            if item['groupByCategory'] == 1 and int(item['idArea']) == int(idArea) :
                df_category = df_category.append(item['slice_stat'], ignore_index=True)
                df_list.append({'idArea' : idArea, 'groupByCategory' : 1,'df' : df_category})

        #print('df_list',df_list)
        print('len(df_list) ->',len(df_list))
        #sys.exit()
        #######################################################################
        # do multiprocessing summarizing slices
        # result container: results2
        #######################################################################

        #pool = mp.Pool(int(cpu_count))
        
        counter = 0
        
        for item in df_list:
        
            counter += 1

            idArea = item['idArea']
            groupByCategory = item['groupByCategory']
            data = item['df']
            print("--> begin running multiprocessing for summarizing slices of each area for groupByCategory", counter)
            results2.append(self.mp_summarize_slices(data, idArea, groupByCategory))
            #pool.apply_async(self.mp_summarize_slices, args=(data, idArea, groupByCategory), callback=self.collect_results2)
            print("--> end running multiprocessing for summarizing slices of each area for groupByCategory", counter)

        #print("close multiprocessing")
        #pool.close()
        #print("join multiprocessing")
        #pool.join()
        #print('results2 -> ',results2)
        #print("finish multiprocessing")
        
        #sys.exit()
        #######################################################################
        # save data
        #######################################################################
        h5 = statistics_save()
        
        
        #for i in df_list:

        for i in results2:
            
            idArea = i[0]
            groupByCategory = i[1]
            data = i[2]
            row = []
            argDict = {}
            print('------------------------------------------------')
            #print("--> save data for area : " + str(idArea))
            #print("--> groupByCategory : " + str(groupByCategory))
            if groupByCategory == 0:
                print("save data grouped by area")
                for item in np.array(data):
                    #print('item[0] -> ',item[0])
                    #print('item[1] -> ',item[1])
                    #print('item[2] -> ',item[2])
                    row.append((item[0], item[1], item[2]))

                argDict['dsName'] = 'baseline_statistic'
                argDict['dType'] = np.dtype({'names':['area','value','count'], 'formats':[np.int32,np.float,np.int32]})

            if groupByCategory == 1:
                print("save data grouped by area and category")
                for item in np.array(data):
                    row.append((item[0], item[1], item[2], item[3]))

                argDict['dsName'] = 'baseline_statistic_groupby_' + str(idCategory)
                argDict['dType'] = np.dtype({'names':['area','category','value','count'], 'formats':[np.int32,np.int8,np.float,np.int32]})

            argDict['fpath'] = configParam['fpath']
            argDict['fname'] = configParam['fname']
            argDict['groupbyCategory'] = groupByCategory
            argDict['idArea'] = idArea
            argDict['idCategory'] = idCategory

            #print('row--> '+str(row))
            #print('argDict--> '+str(argDict))

        h5.save_baseline(argDict, row)
        return 1
