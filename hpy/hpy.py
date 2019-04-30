"""
Copyright (C) 2018-2019 Quasar Science Resources, S.L.
Copyright (C) 2018-2019 Universidad Complutense de Madrid.
Copyright (C) 2018-2019 H2020 ASTERICS

This file is part of HPY.

HPY is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

HPY is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with HPY.  If not, see <http://www.gnu.org/licenses/>.

@package hpy.hpy

--------------------------------------------------------------------------------

This module provides the API for the library
"""

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import numpy as np

from astropy.io import fits

from hpy.utils.fits import from_fits
from hpy.utils.warehouse import warehouse
from hpy.utils.data_container import data_container, Field
from hpy.core.h5table import h5table_writer, h5table_reader
from hpy.core.h5 import h5_writer, h5_reader

from hpy.log import logger
from hpy.log import logger_configuration

LOG_FILE_STR = "conf/log_config.xml"
DEFAULT_GLOBAL_CONFIG = "conf/config.xml"
log = logger().get_log('hpy')

HPY_MODE_MAP = {
    "bfits2hdf5": 0,
    "h5py": 1,
    "pytables": 2
}


HDF5_FORMAT = {
    "bygroups": 0,
    "bytables": 1
}

FITS_MODE_MAP = {
    "astropy": 0,
    "protozfits": 1
}

DEFAULT_MODE = "h5py"
DEFAULT_FITS_MODE = "protozfits"
DEFAULT_HDF5_FORMAT = "bytables"


def init(file_configuration = None, log_file=LOG_FILE_STR,
global_configuration=DEFAULT_GLOBAL_CONFIG):
    return hpy(file_configuration, log_file, global_configuration)

def load_fits(fits_file, fits_mode=DEFAULT_FITS_MODE, test=False):
    if not fits_mode in FITS_MODE_MAP:
        log.error("Unkown mode")
        return
    return hpy().get().load_fits(fits_file, FITS_MODE_MAP[fits_mode],test)

def create_hdf5(fname = None, hpy_mode=DEFAULT_MODE, 
                hdf5_format=DEFAULT_HDF5_FORMAT, **kwargs):
    if not hpy_mode in HPY_MODE_MAP:
        log.error("Unkown mode")
        return
    if not hdf5_format in HDF5_FORMAT:
        log.error("Unknown format")
        return

    m = HPY_MODE_MAP[hpy_mode]
    h5_fmt = HDF5_FORMAT[hdf5_format]

    if m == 0:
        return
    if m == 1 and h5_fmt == 0:
        return hpy().get().create_h5(fname, **kwargs)
    if m == 2 and h5_fmt == 0:
        return hpy().get().create_h5table(fname, **kwargs)

    if m == 1 and h5_fmt == 1:
        return hpy().get().create_h5_tables(fname, **kwargs)
    if m == 2 and h5_fmt == 1:
        return hpy().get().create_h5table_tables(fname, **kwargs)

def load_hdf5(fname, hpy_mode=DEFAULT_MODE):
    if not hpy_mode in HPY_MODE_MAP:
        log.error("Unkown mode")
        return
    m = HPY_MODE_MAP[hpy_mode]
    if m == 0:
        return
    if m == 1:
        return hpy().get().load_h5(fname)
    if m == 2:
        return hpy().get().load_h5table(fname)

def close_hdf5(hpy_mode=DEFAULT_MODE):
    if hpy().get().is_open:
        hpy_mode = hpy().get().mode
    if not hpy_mode in HPY_MODE_MAP:
        log.error("Unkown mode")
        return
    m = HPY_MODE_MAP[hpy_mode]
    if m == 0:
        return 
    if m == 1:
        return hpy().get().close_h5()
    if m == 2:
        return hpy().get().close_h5table()

def get_group(gname, hpy_mode=DEFAULT_MODE):
    if not hpy_mode in HPY_MODE_MAP:
        log.error("Unkown mode")
        return
    m = HPY_MODE_MAP[hpy_mode]
    if m == 0:
        return
    if m == 1:
        return hpy().get().get_group_h5(gname)
    if m == 2:
        return hpy().get().get_group_h5table(gname)

def get_data(dname, hpy_mode=DEFAULT_MODE):
    if not hpy_mode in HPY_MODE_MAP:
        log.error("Unkown mode")
        return
    m = HPY_MODE_MAP[hpy_mode]
    if m == 0:
        return
    if m == 1:
        return hpy().get().get_data_h5(dname)
    if m == 2:
        return hpy().get().get_data_h5table(dname)

def create_r1_from_fits(fits_file, fname = None, v = 'v1'):
    h = hpy().get()
    if not fits_file:
        log.error("No FITS file provided")
        return False
    
    try:
        return from_fits().create_r1(fits_file, fname, v=v)
    except:
        return False

class hpy:
    class __hpy:
        def __init__(self, file_configuration = None, log_file=LOG_FILE_STR,
                     global_configuration=DEFAULT_GLOBAL_CONFIG):
            logger_configuration().load(log_file)
            if file_configuration:
                warehouse(global_configuration).get().load(file_configuration)

        def load_fits(self, fits_file, fits_mode,test=False):
            self._fdata = from_fits().load_r1(fits_file, fits_mode, test)
            return self._fdata
            
        def load_h5(self, fname):
            self._h5 = h5_reader(fname)
            self.is_open = True
            self.mode = "h5py"

        def load_h5table(self, fname):
            self._h5table = h5table_reader(fname)
            self.is_open = True
            self.mode = "pytables"

        def close_h5(self):
            if self._h5:
                self._h5.close()
                self._h5 = None
                self.is_open = False

        def close_h5table(self):
            if self._h5table:
                self._h5table.close()
                self._h5table = None
                self.is_open = False
            
        def get_group_h5(self, gname):
            return self._h5.get_group(gname)

        def get_group_h5table(self, gname):
            return self._h5table.get_group(gname)
        
        def get_data_h5(self, dname):
            return self._h5.get_dataset(dname)
    
        def get_data_h5table(self, dname):
            return self._h5table.get_dataset(dname)

        def create_h5_tables(self, fname, **kwargs):
            if not self._fdata:
                log.error("No data provided")
                return False
            
            h = h5_writer(fname, **kwargs)

            for ext in self._fdata.__dict__:
                gext = h.create_group(ext)
                extfunc = getattr(self._fdata, ext)
                header = h.create_group("header", gext)
                for k in extfunc.header:
                    hg = h.create_group(k, header)
                    h.create_dataset("comment", extfunc.header.comments[k], hg)
                    h.create_dataset("value", extfunc.header[k], hg)

                data = h.create_group("data", gext)
                for col in extfunc.items:
                    self.__create_h5_tables(col, data, h)
            h.close()

            return True

        def __create_h5_tables(self, items, group2fill, h5):
            for k in items.__dict__:
                if 'hpy.utils.fits.ExtensionItem' in \
                   str(type(getattr(items, k))):
                    g = h5.create_group(k, group2fill)
                    self.__create_h5_tables(getattr(items,k),g,h5)
                    continue
                if not isinstance(getattr(items, k), np.ndarray) and \
                   getattr(items, k) == None:
                    continue
                if isinstance(getattr(items, k), np.ndarray) and \
                   getattr(items, k).size == 0:
                    continue
                if isinstance(getattr(items, k), str) and \
                   len(getattr(items, k)) == 0:
                    continue
                
                data = getattr(items, k)
                
                dset = h5.check_dataset(k,group2fill)
                if not dset: 
                
                    if type(data) is fits.column._VLF:
                        log.warning("fits.column._VLF type detected...")
                        log.warning("The table format does not support this type of data...")
                        log.warning("Skipping this data...")
                        continue
                    else:
                        if isinstance(data, np.ndarray):
                            shape = (1,) + data.shape
                            tdata = np.ndarray(shape=shape,dtype=data.dtype)
                            tdata[0] = data

                            h5.create_dataset(k, tdata, group2fill,
                                              chunks=(100,100))
                        elif isinstance(data, str):
                            tdata = np.array([data],dtype=object)
                            
                            dt = h5.create_special_dtype(bytes)
                            dset = h5.create_dataset(k, [tdata], group2fill, 
                                                     dtype=dt,
                                                     chunks=(100,100))
                        else:
                            tdata = np.ndarray(dtype=type(data), shape=(1,))
                            tdata[0] = data
                            
                            h5.create_dataset(k, [tdata], group2fill,
                                              chunks=(100,100))
                else:
                    if isinstance(data, np.ndarray):
                        h5.append_data(data, dset)
                    elif isinstance(data, str):
                        h5.append_data(data, dset)
                    else:
                        h5.append_data([data],dset)

        def __create_table_tables(self, items, group2fill, h5):
            for k in items.__dict__:
                if 'hpy.utils.fits.ExtensionItem' in \
                   str(type(getattr(items, k))):
                    g = h5.create_group(k, group2fill)
                    self.__create_table_groups(getattr(items, k), g, h5)
                    continue
                #log.info("Create dataset %s - %s", k, getattr(items, k))
                if not isinstance(getattr(items, k), np.ndarray) and \
                   getattr(items, k) == None:
                    continue
                if isinstance(getattr(items, k), np.ndarray) and \
                   getattr(items, k).size == 0:
                    continue
                if isinstance(getattr(items, k), str) and \
                   len(getattr(items, k)) == 0:
                    continue
                dclass = type("DClass", (data_container,),{"data": Field(getattr(items,k))})
                dataset = dclass()
                
                dset = h5.check_dataset(k, group2fill)
                if not dset:
                    h5.create_dataset(k, dataset, group2fill)
                else:
                    h5.append_data((dataset, ), dset)

        def create_h5table_tables(self, fname, **kwargs):
            if not self._fdata:
                log.error("No data provided")
                return False
            
            h = h5table_writer(fname, **kwargs)

            for ext in self._fdata.__dict__:
                #log.info(ext)
                gext = h.create_group(ext)
                extfunc = getattr(self._fdata, ext)
                #log.info(extfunc.header)
                header = h.create_group("header", gext)
                for k in extfunc.header:
                    hg = h.create_group(k, header)
                    dclass = type("DClass", (data_container,),{"data": Field(extfunc.header.comments[k])})
                    dataset = dclass()
                    h.create_dataset("comment", dataset, hg)
                    dclass = type("DClass", (data_container,),{"data": Field(extfunc.header[k])})
                    dataset = dclass()
                    h.create_dataset("value", dataset, hg)
                    
                data = h.create_group("data", gext)
                for col in extfunc.items:
                    self.__create_table_tables(col, data, h)
            h.close()
            return True

        def create_h5table(self, fname = None, **kwargs):

            if not self._fdata:
                log.error("No data provided")
                return False

            h = h5table_writer(fname, **kwargs)

            for ext in self._fdata.__dict__:
                #log.info(ext)
                gext = h.create_group(ext)
                extfunc = getattr(self._fdata, ext)
                #log.info(extfunc.header)
                header = h.create_group("header", gext)
                for k in extfunc.header:
                    hg = h.create_group(k, header)
                    dclass = type("DClass", (data_container,),{"data": Field(extfunc.header.comments[k])})
                    dataset = dclass()
                    h.create_dataset("comment", dataset, hg)
                    dclass = type("DClass", (data_container,),{"data": Field(extfunc.header[k])})
                    dataset = dclass()
                    h.create_dataset("value", dataset, hg)
                    
                data = h.create_group("data", gext)
                i = 0
                for col in extfunc.items:
                    #log.info(col)
                    g = h.create_group("%s_%s"%(ext, '{:>08d}'.format(i)), data)
                    self.__create_table_groups(col, g, h)
                    i = i + 1
            h.close()
            return True

        def create_h5(self, fname = None, **kwargs):
            
            if not self._fdata:
                log.error("No data provided")
                return False
            
            h = h5_writer(fname, **kwargs)
            
            for ext in self._fdata.__dict__:
                gext = h.create_group(ext)
                extfunc = getattr(self._fdata, ext)
                header = h.create_group("header", gext)
                for k in extfunc.header:
                    hg = h.create_group(k, header)
                    h.create_dataset("comment", extfunc.header.comments[k], hg)
                    h.create_dataset("value", extfunc.header[k], hg)

                data = h.create_group("data", gext)
                i = 0
                for col in extfunc.items:
                    g = h.create_group("%s_%s"%(ext, '{:>08d}'.format(i)), data)
                    self.__create_h5_groups(col, g, h)
                    i = i + 1
            h.close()
            return True
                        
            
        def create_r1_from_fits(self, fits_file, fname = None, v = 'v1'):
            if not fits_file:
                log.error("No FITS file provided")
                return False

            try:
                return from_fits().create_r1(fits_file, fname, v=v)
            except:
                return False

        def __create_table_groups(self, items, group2fill, h5):
            for k in items.__dict__:
                if 'hpy.utils.fits.ExtensionItem' in \
                   str(type(getattr(items, k))):
                    g = h5.create_group(k, group2fill)
                    self.__create_table_groups(getattr(items, k), g, h5)
                    continue
                #log.info("Create dataset %s - %s", k, getattr(items, k))
                if not isinstance(getattr(items, k), np.ndarray) and getattr(items, k) == None:
                    continue
                if isinstance(getattr(items, k), np.ndarray) and getattr(items, k).size == 0:
                    continue
                if isinstance(getattr(items, k), str) and len(getattr(items, k)) == 0:
                    continue
                dclass = type("DClass", (data_container,),{"data": Field(getattr(items,k))})
                dataset = dclass()
                h5.create_dataset(k, dataset, group2fill)

        def __create_h5_groups(self, items, group2fill, h5):
            for k in items.__dict__:
                if 'hpy.utils.fits.ExtensionItem' in \
                   str(type(getattr(items, k))):
                    g = h5.create_group(k, group2fill)
                    self.__create_h5_groups(getattr(items, k), g, h5)
                    continue
                #log.info("Create dataset %s - %s", k, getattr(items, k))
                if not isinstance(getattr(items, k), np.ndarray) and \
                   getattr(items, k) == None:
                    continue
                if isinstance(getattr(items, k), np.ndarray) and \
                   getattr(items, k).size == 0:
                    continue
                if isinstance(getattr(items, k), str) and \
                   len(getattr(items, k)) == 0:
                    continue
                data = getattr(items, k)
                if type(data) is fits.column._VLF:
                    if data.size > 1:
                        dt = h5.create_special_dtype(np.dtype(data[0].dtype))
                        ds = h5.create_multidataset(k, (data.size,), dt, group2fill)
                        i = 0
                        for d in data:
                            ds[i] = d
                            i = i + 1
                    else:
                        if type(data[0]) is np.ndarray:
                            h5.create_dataset(k, data[0], group2fill, data[0].dtype)
                else:
                    h5.create_dataset(k, getattr(items, k), group2fill)
        
        mode = "h5py"
        is_open = False
        #
        #
        #
    def __init__(self, file_configuration = None, log_file=LOG_FILE_STR,
                 global_configuration=DEFAULT_GLOBAL_CONFIG):
        if not hpy.instance:
            hpy.instance = hpy.__hpy(file_configuration, log_file, global_configuration)
            
    def get(self):
        return hpy.instance
    
    instance = None
