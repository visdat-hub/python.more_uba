# python class for handling the stoffbilanz h5py datasets
# import and export of h5py datasets
#import os, sys
import h5py
import numpy as np

class statistics_save():
    def __init__(self):
        """
        The stb_h5py class contains procedures for import and export the Stoffbilanz h5py datasets.
        """

    #def save_baseline(self, dataset, argDict):
    def save_baseline(self, argDict, statData):

        """
        Save data as h5 file.

        Parameters
        ----------
        dataset : pandas dataframe
            pandas dataframe object
        argDict : json
            The container stores configuration data.
        """

        print("--> write baseline statistic to h5 file")

        #print('save_basestats-->argDict--> '+str(argDict))
        path = argDict['fpath']
        fname = argDict['fname']
        file = path + fname + '.baseline.stats.h5'
        #groupbyCategory = str(argDict['groupbyCategory'])
        idArea = str(argDict['idArea'])
        idCategory = str(argDict['idCategory'])
        tblName = argDict['dsName']
        dType = argDict['dType']

        print('--> save_baseline idArea', str(idArea), file)
        #sys.exit()
        # create structure
        f = h5py.File( file, 'a')

        if f.get('areas') == None:
            grp = f.create_group('areas')
        else:
            grp = f['areas']

        if grp.get(idArea) == None:
            grp_area = grp.create_group(idArea)
        else:
            grp_area = grp[idArea]

        if 'regions' in grp_area:
            grp_area_regions = grp_area['regions']
        else:
            grp_area_regions = grp_area.create_group('regions')

        print('--> grp_area_regions '+str(grp_area_regions))

        for i in grp_area_regions.items():
            #print('i[0]-->'+str(i[0]))
            #print('tblName-->'+str(tblName))
            if i[0] == tblName:
                del grp_area_regions[tblName]
                print('--> delete ' + str(tblName))

                if tblName == 'baseline_statistic':
                    for j in grp_area_regions.items():
                        if j[0] == 'base_statistic':
                            del grp_area_regions['base_statistic']
                            print('--> delete ' + 'base_statistic')

                if tblName == 'baseline_statistic_groupby_'+idCategory:
                    for j in grp_area_regions.items():
                        if j[0] == 'base_statistic_groupby_'+idCategory:
                            del grp_area_regions['base_statistic_groupby_'+idCategory]
                            print('--> delete ' + 'base_statistic_groupby_'+idCategory)

        print('--> create dataset ' + tblName)
        ds = grp_area_regions.create_dataset(tblName, (len(statData),), compression="gzip", compression_opts=9, dtype=dType)
        ds[:] = statData

        f.close()

    def save_basestats(self, dataset, argDict):
        """
        Save base statistic data as h5 file.

        Parameters
        ----------
        dataset : pandas dataframe
            pandas dataframe object
        argDict : json
            The container stores configuration data.
        """
        #print('save_basestats-->argDict--> '+str(argDict))
        path = argDict['fpath']
        fname = argDict['fname']
        file = path + fname + '.baseline.stats.h5'
        idArea = str(argDict['idArea'])
        tblName = argDict['dsName']
        dType = argDict['dType']
        decimalDivider = argDict['decimalDivider']

        print('--> save_basestats idArea', str(idArea))

        # create structure
        f = h5py.File( file, 'a')
        grp = f['areas']
        grp_area = grp[idArea]
        grp_area_regions = grp_area['regions']

        for i in grp_area_regions.items():
            if i[0] == tblName:
                print('--> delete ' + str(tblName))
                del grp_area_regions[tblName]

        print('--> create dataset ', tblName)

        #print(dataset)
        data = np.array(dataset)
        rows = []

        if tblName == 'base_statistic':
            for item in data:
                rows.append((item[0], int(item[7]), (item[4])/decimalDivider, (item[1])/decimalDivider, (item[2])/decimalDivider, (item[6])/decimalDivider, (item[5])/decimalDivider, (item[3])/decimalDivider))
            ds = grp_area_regions.create_dataset(tblName, (len(rows),), compression="gzip", compression_opts=9, dtype=dType)
            ds[:] = rows

        if tblName == 'base_statistic_groupby_' + str(argDict['idCategory']):
            for item in data:
                rows.append((item[0], item[1], item[8], (item[5])/decimalDivider, (item[2])/decimalDivider, (item[3])/decimalDivider, (item[7])/decimalDivider, (item[6])/decimalDivider, (item[4])/decimalDivider))
            ds = grp_area_regions.create_dataset(tblName, (len(rows),), compression="gzip", compression_opts=9, dtype=dType)
            ds[:] = rows
