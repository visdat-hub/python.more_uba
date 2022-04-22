import sys, os
import h5py
import numpy as np
import h5netcdf as nc

if __name__ == "__main__":
	print("calculate atmospheric total N deposition")
	#### input files and variables ####
	# nc files of landuse specific N deposition, resolution 25m, EPSG 31468
	# Stoffbilanz parameter id : 22
	# level 3, resolution 25m
	# *_wat = Gewaesser, 8
	# *_urb = Siedlung, 7, 10, 11, 12, 13, 14
	# *_sem
	# *_oth = Devastierung, 9, 15
	# *_mix = Mischwald, 16
	# *_grs = Gruenland, 2
	# *_dec = Laubwald, 6
	# *_crp = Obst-, Weinbau, 3, 4
	# *_cnf = Nadelwald, 5
	# *_ara = Acker, 1

	path_natm_netcdf = '/mnt/galfdaten/Projekte/2018/LfULG/daten/piniet 3/Variante c/'
	path_landuse = '/mnt/galfdaten/daten_stb/stb_sachsen/parameters/bb01_yearly/6/2015/6_134_3.byte.0.nc'
	path_natm =  '/mnt/galfdaten/daten_stb/stb_sachsen/parameters/bb01_yearly/22/2015/'
	fname_natm = '22_134_3.h5'

	# create h5_natm
	if not os.path.exists(path_natm):
		os.makedirs(path_natm)
	f_h5_natm = h5py.File(path_natm + fname_natm, 'w')

	# read landuse
	f_landuse = h5py.File(path_landuse, 'r')

	# read nc4/hdf5 files of landuse specific N deposition
	f_n_wat = h5py.File(path_natm_netcdf + 'natm_wat.nc', 'r')
	f_n_urb = h5py.File(path_natm_netcdf + 'natm_urb.nc', 'r')
	f_n_sem = h5py.File(path_natm_netcdf + 'natm_sem.nc', 'r')
	f_n_oth = h5py.File(path_natm_netcdf + 'natm_oth.nc', 'r')
	f_n_mix = h5py.File(path_natm_netcdf + 'natm_mix.nc', 'r')
	f_n_grs = h5py.File(path_natm_netcdf + 'natm_grs.nc', 'r')
	f_n_dec = h5py.File(path_natm_netcdf + 'natm_dec.nc', 'r')
	f_n_crp = h5py.File(path_natm_netcdf + 'natm_crp.nc', 'r')
	f_n_cnf = h5py.File(path_natm_netcdf + 'natm_cnf.nc', 'r')
	f_n_ara = h5py.File(path_natm_netcdf + 'natm_ara.nc', 'r')

	# read bands
	ds_landuse = f_landuse['Band1']
	ds_n_wat = f_n_wat['Band1']
	ds_n_urb = f_n_urb['Band1']
	ds_n_sem = f_n_sem['Band1']
	ds_n_oth = f_n_oth['Band1']
	ds_n_mix = f_n_mix['Band1']
	ds_n_grs = f_n_grs['Band1']
	ds_n_dec = f_n_dec['Band1']
	ds_n_crp = f_n_crp['Band1']
	ds_n_cnf = f_n_cnf['Band1']
	ds_n_ara = f_n_ara['Band1']

	total_n_atm = np.full((ds_landuse.shape), 0).astype(float)
	total_n_atm[total_n_atm == -99999] = np.NaN
	print("stack natm layer")
	ds_stack = np.stack((ds_landuse, ds_n_ara, ds_n_grs, ds_n_crp, ds_n_cnf, ds_n_dec, ds_n_urb, ds_n_wat, ds_n_oth, ds_n_mix))
	ds_stack[ds_stack == -99999.0] = np.NaN
	print("assign natm to landuse")
	total_n_atm = np.nansum([total_n_atm, np.array(np.where( ds_stack[0] == 1, ds_stack[1], np.NaN ))], axis=0)
	total_n_atm = np.nansum([total_n_atm, np.array(np.where( ds_stack[0] == 2, ds_stack[2], np.NaN ))], axis=0)
	total_n_atm = np.nansum([total_n_atm, np.array(np.where( np.isin( ds_stack[0], [3, 4]), ds_stack[3], np.NaN ))], axis=0)
	total_n_atm = np.nansum([total_n_atm, np.array(np.where( ds_stack[0] == 5, ds_stack[4], np.NaN ))], axis=0)
	total_n_atm = np.nansum([total_n_atm, np.array(np.where( ds_stack[0] == 6, ds_stack[5], np.NaN ))], axis=0)
	total_n_atm = np.nansum([total_n_atm, np.array(np.where( np.isin( ds_stack[0], [7, 10, 11, 12, 13, 14]), ds_stack[6], np.NaN ))], axis=0)
	total_n_atm = np.nansum([total_n_atm, np.array(np.where( ds_stack[0] == 8, ds_stack[7], np.NaN ))], axis=0)
	total_n_atm = np.nansum([total_n_atm, np.array(np.where( np.isin( ds_stack[0], [9, 15]), ds_stack[8], np.NaN ))], axis=0)
	total_n_atm = np.nansum([total_n_atm, np.array(np.where( ds_stack[0] == 16, ds_stack[9], np.NaN ))], axis=0)
	total_n_atm[total_n_atm == 0] = np.NaN
	# Umrechnung von eq nach kg/ha/a
	func = lambda x: x / 1000 * 14.0
	print("1")
	total_n_atm = np.array(np.where(np.isnan(total_n_atm) == False, func(total_n_atm), np.nan))
	print("2")
	
	# write hdf5 file
	geotransform = ds_landuse.attrs['GeoTransform']
	print("3")
	projection = ds_landuse.attrs['Projection']
	print("4")
	ds = f_h5_natm.create_dataset('Band1', (total_n_atm.shape), data = total_n_atm, compression="gzip", compression_opts=9)
	print("5")
	ds.attrs['GeoTransform'] = geotransform
	print("6")
	ds.attrs['Projection'] = projection
	print("7")
	# dataset x
	ds_x = f_landuse['x']
	ds = f_h5_natm.create_dataset('x', (ds_x.shape), data = ds_x)
	# dataset y
	ds_y = f_landuse['y']
	ds = f_h5_natm.create_dataset('y', (ds_y.shape), data = ds_y)
