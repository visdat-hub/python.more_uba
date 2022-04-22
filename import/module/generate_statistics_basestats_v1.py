# class base do_statistics
import math
import os
import h5py
import numpy as np
import pandas as pd

class generate_statistics_basestats_v1():
    def __init__(self):
        print("class generate_statistics_basestats_v1")

    def run(self, f_param_array, f_category, f_area_array, totalRegion, stat):
        print("------------------------------")
        print("run base statistic calculation")
        # f_param[0] is pointer to h5 data, f_param[1] -> decimals, f_param[2] -> dataType
        result = None

        # without area subclasses
        if totalRegion == 1:
            print("area subclasses: no")
            # without category subclasses ==> false
            if f_category == -1:
                print("category subclasses: no")
                result = self.run_a0_c0(f_param_array, f_area_array, stat)

            # with category subclasses
            if f_category != -1:
                print("category subclasses: yes")
                result = self.run_a0_c1(f_param_array, f_area_array, f_category)

        # with area subclasses ==> true
        if totalRegion == 0:
            print("area subclasses: yes")
            # without category subclasses
            if f_category == -1:
                print("category subclasses: no")
                result = self.run_a1_c0(f_param_array, f_area_array, stat)

            # with category subclasses
            if f_category != -1:
                print("category subclasses: yes")
                result = self.run_a1_c1(f_param_array, f_area_array, f_category)
        print("------------------------------")
        return result, totalRegion

    # area subclasses == false (totalRegion = 1), category subclasses == false
    def run_a0_c0(self, f_param_array, f_area_array, stat):
        my_datatype = None
        rows = []
        decimalDivider = self.check_decimals(f_param_array)

        print('--> f_param_array: '+ str(f_param_array))
        print('--> decimalDivider: '+ str(decimalDivider))

        #dataType = f_param_array[2]
        f_param = f_param_array[0]
        param_band1 = f_param['Band1']
        f_area = f_area_array[0]
        area_band1 = f_area['Band1']
        noData_param = param_band1.attrs['_FillValue']
        noData_area = area_band1.attrs['_FillValue']
        np_param = np.array(param_band1).astype(float)
        np_area = np.array(f_area['Band1']).astype(float)

        np_param[np_param == noData_param] = np.nan
        np_area[np_area == noData_area] = np.nan

        np_ar = np.stack((np_area, np_param))
        np_ar = np.where(np.isnan(np_ar[0]) == False, np_ar[1], np.nan)

        df = pd.DataFrame({'value' : np_ar.flatten()})
        df_stat = df['value'].agg(['count','mean','min','max','std','median','sum']).reset_index()
        df_stat['value'] = df_stat['value'] / decimalDivider

        data = np.array([df_stat['index'], df_stat['value']])

        rows.append((data[1][0], (data[1][1]), (data[1][2]), (data[1][3]), (data[1][4]), (data[1][5]), (data[1][6])))
        
        
        my_datatype = np.dtype([('count', np.int32),('mean', np.float),('min', np.float),('max', np.float),('std', np.float),('median', np.float),('sum', np.float)])

        result = rows, my_datatype, 'base_statistic'

        # write data to h5 file
        if len(result[0][0]) > 0:
            self.write_statistics2h5(f_param_array, f_area_array, result)
        else:
            print("WARNING -> length of result: 0")

    # area subclasses == false (totalRegion = 1), category subclasses == true
    def run_a0_c1(self, f_param_array, f_area_array, f_category):
        my_datatype, idCategory = None, None
        rows = []
        decimalDivider = self.check_decimals(f_param_array)

        print('--> f_param_array: '+ str(f_param_array))
        print('--> decimalDivider: '+ str(decimalDivider))

        #dataType = f_param_array[2]
        f_param = f_param_array[0]
        param_band1 = f_param['Band1']
        f_area = f_area_array[0]
        area_band1 = f_area['Band1']
        category_band1 = f_category['Band1']
        noData_param = param_band1.attrs['_FillValue']
        noData_area = area_band1.attrs['_FillValue']
        noData_category = category_band1.attrs['_FillValue']
        np_param = np.array(param_band1).astype(float)
        np_area = np.array(f_area['Band1']).astype(float)
        np_category = np.array(f_category['Band1']).astype(float)

        np_param[np_param == noData_param] = np.nan
        np_area[np_area == noData_area] = np.nan
        np_category[np_category == noData_category] = np.nan

        np_ar = np.array([np_area.flatten(), np_category.flatten(), np_param.flatten()])

        df = pd.DataFrame(np_ar[0,:], columns=['area'])
        df['category'] = np_ar[1,:]
        df['value'] = np_ar[2,:]
        df = df[pd.notnull(df['area'])]
        df = df[pd.notnull(df['value'])]
        df_stat = df.groupby(['category'])['value'].agg(['count','mean','min','max','std','median','sum']).reset_index()
        data = np.array(df_stat)

        for item in data:
            rows.append((item[0], item[1], (item[2])/decimalDivider, (item[3])/decimalDivider, (item[4])/decimalDivider, (item[5])/decimalDivider, (item[6])/decimalDivider, (item[7])/decimalDivider))
        
        my_datatype = np.dtype([('category', np.int16),('count', np.int32),('mean', np.float),('min', np.float),('max', np.float),('std', np.float),('median', np.float),('sum', np.float)])

        category_fname = os.path.basename(f_category.filename)
        idCategory = str(category_fname.split('_')[0])

        result = rows, my_datatype, 'base_statistic_groupby_'+ str(idCategory)

        # write data to h5 file
        if len(result[0][0]) > 0:
            self.write_statistics2h5(f_param_array, f_area_array, result)
        else:
            print("WARNING -> length of result: 0")

    # area subclasses == true (totalRegion = 0), category subclasses == false
    def run_a1_c0(self, f_param_array, f_area_array, stat):
        my_datatype = None, None
        rows = []
        decimalDivider = self.check_decimals(f_param_array)

        print('--> f_param_array: '+ str(f_param_array))
        print('--> decimalDivider: '+ str(decimalDivider))

        #dataType = f_param_array[2]
        f_param = f_param_array[0]
        param_band1 = f_param['Band1']
        f_area = f_area_array[0]
        area_band1 = f_area['Band1']
        noData_param = param_band1.attrs['_FillValue']
        noData_area = area_band1.attrs['_FillValue']
        np_param = np.array(param_band1).astype(float)
        np_area = np.array(f_area['Band1']).astype(float)

        np_param[np_param == noData_param] = np.nan
        np_area[np_area == noData_area] = np.nan

        np_ar = np.stack((np_area, np_param))

        df = pd.DataFrame({'area' : np_ar[0].flatten(), 'value' : np_ar[1].flatten()})
        df = df[pd.notnull(df['area'])]

        df_stat = df.groupby(['area'])['value'].agg(['count','mean','min','max','std','median','sum']).reset_index()
        my_datatype = np.dtype([('area', np.int32),('count', np.int32),('mean', np.float),('min', np.float),('max', np.float),('std', np.float),('median', np.float),('sum', np.float)])
        data = np.array(df_stat)
        rows = []
        for item in data:
            if stat == 'avg':
                rows.append((item[0], item[1], (item[2])/decimalDivider, (item[3])/decimalDivider, (item[4])/decimalDivider, (item[5])/decimalDivider, (item[6])/decimalDivider, (item[7])/decimalDivider))
            if stat == 'sum':
                rows.append((item[0], item[1], (item[7])/decimalDivider, (item[7])/decimalDivider, (item[7])/decimalDivider, (item[7])/decimalDivider, (item[7])/decimalDivider, (item[7])/decimalDivider))

        result = rows, my_datatype, 'base_statistic'

        # write data to h5 file
        if len(result[0][0]) > 0:
            self.write_statistics2h5(f_param_array, f_area_array, result)
        else:
            print("WARNING -> length of result: 0")

    # area subclasses == true (totalRegion = 0), category subclasses == true
    def run_a1_c1(self, f_param_array, f_area_array, f_category, stat):
        my_datatype, idCategory =  None, None
        rows = []

        decimalDivider = self.check_decimals(f_param_array)

        print('--> f_param_array: '+ str(f_param_array))
        print('--> decimalDivider: '+ str(decimalDivider))

        #dataType = f_param_array[2]
        f_param = f_param_array[0]
        param_band1 = f_param['Band1']
        f_area = f_area_array[0]
        area_band1 = f_area['Band1']
        category_band1 = f_category['Band1']
        noData_param = param_band1.attrs['_FillValue']
        noData_area = area_band1.attrs['_FillValue']
        noData_category = category_band1.attrs['_FillValue']
        np_param = np.array(param_band1).astype(float)
        np_area = np.array(f_area['Band1']).astype(float)
        np_category = np.array(f_category['Band1']).astype(float)

        np_param[np_param == noData_param] = np.nan
        np_area[np_area == noData_area] = np.nan
        np_category[np_category == noData_category] = np.nan

        np_ar = np.array([np_area.flatten(), np_category.flatten(), np_param.flatten()])
        df = pd.DataFrame(np_ar[0,:], columns=['area'])
        df['category'] = np_ar[1,:]
        df['value'] = np_ar[2,:]
        df = df[pd.notnull(df['area'])]
        df = df[pd.notnull(df['value'])]

        df_stat = df.groupby(['area','category'])['value'].agg(['count','mean','min','max','std','median','sum']).reset_index()
        data = np.array(df_stat)

        for item in data:
            rows.append((item[0], item[1], item[2], (item[3])/decimalDivider, (item[4])/decimalDivider, (item[5])/decimalDivider, (item[6])/decimalDivider, (item[7])/decimalDivider, (item[8])/decimalDivider))
        
        my_datatype = np.dtype([('area', np.int32),('category', np.int16),('count', np.int32),('mean', np.float),('min', np.float),('max', np.float),('std', np.float),('median', np.float),('sum', np.float)])

        category_fname = os.path.basename(f_category.filename)
        idCategory = str(category_fname.split('_')[0])

        result =  rows, my_datatype, 'base_statistic_groupby_'+ str(idCategory)

        # write data to h5 file
        if len(result[0][0]) > 0:
            self.write_statistics2h5(f_param_array, f_area_array, result)
        else:
            print("WARNING -> length of result: 0")

    def check_decimals(self, f_param):
        decimalDivider = 1
        decimals = float(f_param[1])
        dataType = f_param[4]
        print('--> decimals : '+str(decimals))
        print('--> dataType : '+str(dataType))
        if dataType in ["int8", "int16", "int32", "int64"] and decimals != 0:
            decimalDivider = math.pow(10,decimals)
            print("decimalDivider: " + str(decimalDivider))
        return decimalDivider

    def write_statistics2h5(self, f_param_array, f_area_array, data):
        print("write statistics to h5 file")

        path = f_param_array[2]
        fname = f_param_array[3] + '.stats.h5'
        idArea = str(f_area_array[4])

        #totalRegion = data[1]
        tblName = data[2]
        dtype = data[1]
        statData = data[0]

        # create structure
        f = h5py.File( path + fname, 'a')
        if f.get('areas') == None:
            grp = f.create_group('areas')
        else:
            grp = f['areas']
        if grp.get(idArea) == None:
            grp_area = grp.create_group(idArea)
            #grp_area_total = grp_area.create_group('total')
            grp_area_regions = grp_area.create_group('regions')
        else:
            #grp_area_total = grp['/areas/' + idArea + '/total']
            grp_area_regions = grp['/areas/' + idArea + '/regions']

        # delete table if exists and create new dataset
        for i in grp_area_regions.items():
            #print(str(i)+' == '+ tblName)
            if i[0] == tblName:
                del grp_area_regions[tblName]
        ds = grp_area_regions.create_dataset(tblName, (len(statData),), compression="gzip", compression_opts=9, dtype=dtype)
        ds[:] = statData

        f.close()

    def close_files(self, files):
        for f in files:
            if f != -1:
                print(f)
                f.close()
                print(f)
