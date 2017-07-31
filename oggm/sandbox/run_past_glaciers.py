"""Run with a subset of benchmark glaciers"""
from __future__ import division

# Log message format
import logging
logging.basicConfig(format='%(asctime)s: %(name)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

# Module logger
log = logging.getLogger(__name__)

# Python imports
import os
from glob import glob
import shutil

#  Libs
import geopandas as gpd
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import xarray as xr

# Locals
import oggm.cfg as cfg
from oggm import workflow
from oggm import tasks
from oggm.workflow import execute_entity_task
from oggm import graphics, utils


# Regions:
# Arctic Canada North 03
# Arctic Canada South 04

rgi_reg = '03'  #

# Some globals for more control on what to run
RUN_GIS_PREPRO = True # run GIS preprocessing tasks (before climate)
RUN_CLIMATE_PREPRO = True # run climate preprocessing tasks
RUN_INVERSION = True # run bed inversion
RUN_PAST = True
RUN_GLACIER = True
RUN_SELECTION = True # false= it runs for the whole region

# Initialize OGGM
cfg.initialize()

# Local paths (where to write output and where to download input)
WORKING_DIR = '/mnt/raid1/home/avlug/Workspace/A03_test/'
DATA_DIR = '/mnt/raid1/home/avlug/oggm-data/'
CESM_DIR = '/mnt/raid1/home/avlug/CESMoggm/selection1/'

cfg.PATHS['working_dir'] = WORKING_DIR
cfg.PATHS['topo_dir'] = os.path.join(DATA_DIR, 'topo')
cfg.PATHS['gcm_dir'] = os.path.join(CESM_DIR)
cfg.PATHS['rgi_dir'] = os.path.join(DATA_DIR, 'rgi')
cfg.PATHS['cru_dir'] = os.path.join(DATA_DIR, 'cru')

# Currently OGGM wants some directories to exist
# (maybe I'll change this but it can also catch errors in the user config)
utils.mkdir(cfg.PATHS['working_dir'])
utils.mkdir(cfg.PATHS['topo_dir'])
utils.mkdir(cfg.PATHS['cru_dir'])
utils.mkdir(cfg.PATHS['rgi_dir'])

# Use multiprocessing?
cfg.PARAMS['use_multiprocessing'] = True
cfg.PARAMS['continue_on_error'] = True
# Other params
cfg.PARAMS['temp_use_local_gradient'] = False
cfg.PARAMS['invert_with_sliding'] = False
cfg.PARAMS['bed_shape'] = 'mixed'

#
cfg.PARAMS['optimize_inversion_params'] = False
cfg.PARAMS['inversion_glen_a'] = cfg.A * 3
cfg.PARAMS['inversion_fs'] = 0.
cfg.PARAMS['dmax'] = 230
cfg.PARAMS['border'] = 200


# Parameters for glacier evolution run
cfg.PARAMS['y0'] = 851
cfg.PARAMS['nyears'] = 1155
cfg.PARAMS['temp_suffix'] = '.TREFHT.085001-200512.nc'
cfg.PARAMS['precc_suffix'] = '.PRECC.085001-200512.nc'
cfg.PARAMS['precl_suffix'] = '.PRECL.085001-200512.nc'

# # # Download RGI files
rgi_dir = utils.get_rgi_dir()
rgi_shp = list(glob(os.path.join(rgi_dir, "*", rgi_reg+ '_rgi50_*.shp')))
assert len(rgi_shp) == 1
rgidf = gpd.read_file(rgi_shp[0])

print(cfg.PATHS)

flink, mbdatadir = utils.get_wgms_files()
ids_with_mb = pd.read_csv(flink)['RGI50_ID'].values


if RUN_SELECTION:
    keep_ids = ['RGI50-03.04079', 'RGI50-03.02440',
                'RGI50-03.04539', 'RGI50-03.01427',
                'RGI50-03.01623', 'RGI50-03.00001', 'RGI50-03.00002',
                'RGI50-03.00003']
    keep_indexes = [(i in keep_ids) for i in rgidf.RGIId]
    rgidf = rgidf.iloc[keep_indexes]

# Go - initialize working directories
gdirs = workflow.init_glacier_regions(rgidf, reset=True, force=True)
# gdirs = workflow.init_glacier_regions(rgidf, reset=False)
log.info('Number of glaciers: {}'.format(len(rgidf)))

# Prepro tasks
#
if RUN_GIS_PREPRO:
    workflow.gis_prepro_tasks(gdirs)

if RUN_CLIMATE_PREPRO:
    execute_entity_task(tasks.process_cru_data, gdirs)
    tasks.compute_ref_t_stars(gdirs)
    tasks.distribute_t_stars(gdirs)
#
if RUN_INVERSION:
    execute_entity_task(tasks.prepare_for_inversion, gdirs)
    tasks.optimize_inversion_params(gdirs) # No Canadian Arctic glacier in the GlaTiDa file
    execute_entity_task(tasks.volume_inversion, gdirs)
    execute_entity_task(tasks.init_present_time_glacier, gdirs)

if RUN_PAST:
    for ens in (np.arange(13) + 1):
        ensemble_member = ''.join(['b.e11.BLMTRC5CN.f19_g16.',
                                       str(ens).zfill(3), '.cam.h0'])
        execute_entity_task(tasks.process_cesm_data, gdirs,
                            filesuffix=ensemble_member)

if RUN_GLACIER:
    for ens in (np.arange(13) + 1):
        print('ens:', ens)
        ensemble_member = ''.join(['b.e11.BLMTRC5CN.f19_g16.',
                                       str(ens).zfill(3), '.cam.h0'])
        execute_entity_task(tasks.glacier_evolution, gdirs,
                            filesuffix=ensemble_member)



