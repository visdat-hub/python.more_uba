# python class for calculating base statistics
import os
import sys
import h5py
import numpy as np
import math
import pandas as pd
from statsmodels.stats.weightstats import DescrStatsW
from module.statistics_save import statistics_save

class generate_statistics_basestats_v2():
    def __init__(self):
        """
        The stb_statistics class contains procedures for calculating the STOFFBILANZ base statistics.
        """

    def run(self, configParam, configCategory, configArea, idArea, stat):
        """
        Calculating STOFFBILANZ base statistic from baseline statistics and save data to hdf5 statistic file.

        Parameters
        ----------
        config: json
            baseline configuration
        """

        f_category = configCategory['h5py_file']
        idCategory = configCategory['idCategory']

        h5 = statistics_save()

        if f_category == -1:

            dsName = 'baseline_statistic'
            ds, argDict = self.get_baseline_dataset(configParam, dsName, idArea, -1)
            baseStats = self.base_statistics(ds, argDict, stat)
            argDict['fpath'] = configParam['fpath']
            argDict['fname'] = configParam['fname']
            argDict['dsName'] = 'base_statistic'
            argDict['dType'] = np.dtype([('area', np.int32),('count', np.int64),('mean', np.float),('min', np.float),('max', np.float),('std', np.float),('median', np.float),('sum', np.float)])
            h5.save_basestats( baseStats, argDict)

        else:

            dsName = 'baseline_statistic_groupby_' + str(idCategory)
            ds, argDict = self.get_baseline_dataset(configParam, dsName, idArea, idCategory)
            baseStats = self.base_statistics(ds, argDict, stat)
            argDict['fpath'] = configParam['fpath']
            argDict['fname'] = configParam['fname']
            argDict['dsName'] = 'base_statistic_groupby_' + str(idCategory)
            argDict['dType'] = np.dtype([('area', np.int32),('category', np.int16),('count', np.int32),('mean', np.float),('min', np.float),('max', np.float),('std', np.float),('median', np.float),('sum', np.float)])
            h5.save_basestats( baseStats, argDict)

    def get_baseline_dataset(self, argDict, dsName, idArea, idCategory):
        """
        Read baseline dataset from hdf5 file.

        Parameters
        ----------
        argDict : json
            configuration data
        dsName: string
            name of hdf5 dataset

        Returns
        ----------
        ds : numpy array
            dataset containing the unique counts of values
        argDict: json
            dictionary for various parameters
        """

        #print('-->argDict : '+str(argDict))

        fname = argDict['fname']
        paramPath = argDict['fpath'] + fname + '.baseline.stats.h5'
        decimals = float(argDict['decimals'])
        decimalDivider  = math.pow(10, decimals)
        #print('decimalDivider-->'+str(decimalDivider))
        #print('paramPath-->'+str(paramPath))

        # read file
        if os.path.isfile(paramPath):
            f_param = h5py.File(paramPath, 'r')
        else:
            print('file not found ' + paramPath)
            sys.exit()

        ds_path = '/areas/' + str(idArea) + '/regions/' + dsName
        #print('ds_path--> ' + ds_path)
        ds = f_param[ds_path][:]
        df = {'values' : [], 'areas' : [], 'category' : [], 'counts' : []}
        # grouped by area and values
        if len(ds[0]) == 3:
            for i in ds:
                # if count > 0
                if i[2] > 0:
                    df['areas'].append(i[0])
                    df['values'].append(i[1])
                    df['counts'].append(i[2])
        # grouped by areas, categories and values
        if len(ds[0]) == 4:
            for i in ds:
                # if count > 0
                if i[3] > 0:
                    df['areas'].append(i[0])
                    df['category'].append(i[1])
                    df['values'].append(i[2])
                    df['counts'].append(i[3])

        argDict = {}
        argDict['decimalDivider'] = decimalDivider
        argDict['idCategory'] = idCategory
        argDict['idArea'] = idArea
        print('--> get area '+str(idArea)+' / category '+str(idCategory)+' ... done')
        print('--> get dataset ... done')

        #print('argDict-->'+str(argDict))
        #print('df-->'+str(df))
        return df, argDict

    def base_statistics(self, ds, argDict, stat):
        """
        Calculating base statistics.

        Parameters
        ----------
        ds : numpy array
            dataset containing the unique counts of values
        argDict : json
            additional configuration parameters

        Returns
        ----------
        r : pandas dataframe
            A pandas dataframe containing the base statistics.
        """

        #print('ds--> '+str(ds))
        #print("ds_areas--> "+str(ds['areas']))
        #print("ds_category--> "+str(ds['category']))
        #print("ds_areas_len--> "+str(len(ds['areas'])))
        #print("ds_category_len--> "+str(len(ds['category'])))

        r = pd.DataFrame()

        if len(ds['areas']) > 0:
            if len(ds['category']) == 0:
                ar = np.array([ds['areas'], ds['values'], ds['counts']])
                df = pd.DataFrame(ar.T , columns=['areas','values','counts'])
                df['prod'] = df['values'] * df['counts']
                
                count = df.groupby(['areas'])['counts'].agg(lambda x: sum(x))
                summe = df.groupby(['areas']).apply(lambda x: np.sum(x['values'] * x['counts']))
                
                if stat == 'avg':
                    
                    minmax = df.groupby(['areas'])['values'].agg([('min', lambda x: min(x)),('max', lambda x: max(x))])
                    # calculate if not zero
                    weighted_avg = df.groupby(['areas']).apply(lambda x: np.average(x['values'], weights=x['counts'])  if x['counts'].all() else 0)
                    weighted_std = df.groupby(['areas']).apply(lambda x: (DescrStatsW(x['values'], weights=x['counts'], ddof=0).std))
                    weighted_median = df.groupby(['areas']).apply(lambda x: self.calculate_median(x))
                    
                    r['min'] = minmax['min']
                    r['max'] = minmax['max']
                    r['sum'] = summe
                    r['avg'] = weighted_avg
                    r['median'] = weighted_median
                    r['std'] = weighted_std
                    r['count'] =  count
                    r['idarea'] = argDict['idArea']
                    r['category_param'] = None
                    r['category'] = None
                
                if stat == 'sum':
                    r['min'] = summe
                    r['max'] = summe
                    r['sum'] = summe
                    r['avg'] = summe
                    r['median'] = summe
                    r['std'] = summe
                    r['count'] =  count
                    r['idarea'] = argDict['idArea']
                    r['category_param'] = None
                    r['category'] = None

            if len(ds['category']) == len(ds['areas']):
                ar = np.array([ds['areas'], ds['category'], ds['values'], ds['counts']])
                df = pd.DataFrame(ar.T , columns=['areas','category','values','counts'])
                df = df[~df['counts'].isin([0])]
                df = df[~df['values'].isin([-9999])]
                df['prod'] = df['values'] * df['counts']

                minmax = df.groupby(['areas','category'])['values'].agg([('min', lambda x: min(x)),('max', lambda x: max(x))])
                weighted_avg = df.groupby(['areas','category']).apply(lambda x: np.average(x['values'], weights=x['counts']))
                count = df.groupby(['areas','category'])['counts'].agg(lambda x: sum(x))
                weighted_std = df.groupby(['areas','category']).apply(lambda x: (DescrStatsW(x['values'], weights=x['counts'], ddof=0).std))
                weighted_median = df.groupby(['areas','category']).apply(lambda x: self.calculate_median(x))
                summe = df.groupby(['areas','category']).apply(lambda x: np.sum(x['values'] * x['counts']))
                r['min'] = minmax['min']
                r['max'] = minmax['max']
                r['sum'] = summe
                r['avg'] = weighted_avg
                r['median'] = weighted_median
                r['std'] = weighted_std
                r['count'] =  count
                r['idarea'] = argDict['idArea']
                r['category_param'] = argDict['idCategory']
            print("--> calculate statistics ... done")
        else:
            print("--> ERROR: EMPTY DATASET")
        return r.reset_index()

    def calculate_median(self, df):
        """
        Calculation of weighted median from unique counts.

        Parameters
        ----------
        df : array
            dataset containing the unique counts of values

        Returns
        ----------
        median : array
            weighted median grouped by area, (category)
        """
        pd_df = pd.DataFrame({'value' : df['values'], 'count' : df['counts']})
        median = None
        pd_df = pd_df.sort_values(by=['value']) # sortiere Werte
        pd_df['cumsum'] = np.cumsum(pd_df['count']) # berechne die kumulative Summe ueber die Anzahl der Werte
        ds_halflen = math.floor(np.max(pd_df['cumsum']) / 2) # ermittle die Datenmitte
        last_row = pd_df[pd_df['cumsum'] <= ds_halflen].tail(1) # finde den Wert unterhalb der Mitte
        first_row = pd_df[pd_df['cumsum'] >= ds_halflen].head(1) # finde den Wert oberhalb der Mitte
        first_diff = (first_row['cumsum'] - ds_halflen).iloc[0]
        if len(last_row) > 0:
            last_diff = (ds_halflen - last_row['cumsum']).iloc[0]
            if first_diff >= last_diff:
                median = last_row['value'].iloc[0]
            else:
                median = first_row['value'].iloc[0]
        else:
            median = pd_df.iloc[0]['value']

        if median == None:
            median = 0

        return median
