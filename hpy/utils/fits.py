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

@package hpy.utils.fits

--------------------------------------------------------------------------------

This module provides the utilities to reads FITS files
"""

import os

from astropy.io import fits
from hpy.core.h5 import h5_writer
import numpy as np
from hpy.utils.warehouse import warehouse
from hpy.log import logger

from protozfits import File

from hpy.utils.data_container import data_container as Cnt
from hpy.utils.data_container import schema as Sc

PROTOZFITS_STR = "protozfits."

TEST_EVENTS_NUMBER = 100
TEST_EXTENSION = 'CameraConfig'

class from_fits:

    def load_r1(self, fits_file, fits_mode=1, test=False):
        if not fits_file:
            self.log.error("No FITS file provided")
            return False
        if not os.path.isfile(fits_file):
            self.log.error("Bad file provided")
            return False
        self.log.info("Loading FITS file %s in mode %d", fits_file, fits_mode)
        if fits_mode == 0:
            try:
                f = fits.open(fits_file)
            except:
                self.log.error("Invalid FITS file provided")
                return False
            sc = self.__load_schema_from_astropy(f,test)
            f.close()
            return sc
        if fits_mode == 1:
            try:
                f = File(fits_file)
            except OSError:
                self.log.error("Invalid FITS file provided")
                return False
            sc = self.__load_schema_from_protozfits(f,test)
            f.close()
            return sc

    def __load_schema_from_protozfits(self, fits_file, test=False):
        ret = type("FData", (Sc,),{})()

        for ext in fits_file.__dict__:
            self.log.info(ext)
            if self._def and not ext in self._def:
                continue
            extfunc = getattr(fits_file, ext)
            # FIXME: Remove this if
            #if ext == 'Events':
            #    continue
            ext_obj = type("Extension", (Sc,),{})()
            setattr(ret, ext, ext_obj)
            setattr(ext_obj, "header", extfunc.header)
            setattr(ext_obj, "items", [])
            if test:
                i = 0
                for col in extfunc:
                    if i == TEST_EVENTS_NUMBER: 
                        break
                    i = i + 1
                    ext_item = type("ExtensionItem", (Sc,),{})()
                    ext_obj.items.append(ext_item)
                    for k in col._asdict():
                        self.__load_subdata_from_protozfits(k, getattr(col, k), ext_item)
                continue
            #i = 0
            for col in extfunc:
                #if i == 10: 
                #    return ret
                #i = i + 1
                #self.log.info(col)
                ext_item = type("ExtensionItem", (Sc,),{})()
                ext_obj.items.append(ext_item)
                for k in col._asdict():
                    self.__load_subdata_from_protozfits(k, getattr(col, k), ext_item)
        return ret
    
    def __load_subdata_from_protozfits(self, name, data, obj):
        if PROTOZFITS_STR not in str(type(data)):
            self.log.info("%s - %s", name, data)
            setattr(obj, name, data)
            return 
        item = type("ExtensionItem", (Sc,),{})()
        setattr(obj, name, item)
        for k in data._asdict():
            self.__load_subdata_from_protozfits(k, getattr(data, k),item)

    def __load_schema_from_astropy(self, fits_file, test=False):
        #self.log.info(self._def)
        if self._def:
            return self.__filter_schema_from_astropy(fits_file, test)

        ret = type("FData", (Sc,),{})()

        for ext in fits_file:

            if fits_file[ext.name].size <= 0:
                continue
            if test:
                if ext.name == TEST_EXTENSION:
                    ext_obj = type("Extension", (Sc,),{})()
                    setattr(ret, ext.name, ext_obj)
                    setattr(ext_obj, "header", fits_file[ext.name].header)
                    setattr(ext_obj, "items", [])
                    item = type("ExtensionItem", (Sc,),{})()
                    ext_obj.items.append(item)
                    for name in fits_file[ext.name].columns.names:
                        setattr(item, name, fits_file[ext.name].data[name])
                    break
            ext_obj = type("Extension", (Sc,),{})()
            setattr(ret, ext.name, ext_obj)
            setattr(ext_obj, "header", fits_file[ext.name].header)
            setattr(ext_obj, "items", [])
            item = type("ExtensionItem", (Sc,),{})()
            ext_obj.items.append(item)
            for name in fits_file[ext.name].columns.names:
                setattr(item, name, fits_file[ext.name].data[name])

        return ret
        
    def __filter_schema_from_astropy(self, f, test):
        ret = type("FData", (Sc,), {})()

        for ext in f:
            if not ext.name in self._def:
                continue
            if f[ext.name].size <= 0:
                continue
            ext_obj = type("Extension", (Sc,),{})()
            setattr(ret, ext.name, ext_obj)
            setattr(ext_obj, "header", f[ext.name].header)
            setattr(ext_obj, "items", [])
            item = type("ExtensionItem", (Sc,), {})()
            ext_obj.items.append(item)
            for name in f[ext.name].columns.names:
                item2fill = item
                self.log.info(name)
                if not name in self._def[ext.name]:
                    self.log.info("Key not found in the definition file")
                else:
                    if 'att' in self._def[ext.name][name] and \
                       'group' in self._def[ext.name][name]['att']:
                        g = self._def[ext.name][name]['att']['group']
                        item2fill = self.__load_subdata_filter_astropy(ext.name,
                                                                       name,
                                                                       item2fill)
                #self.log.info("%s - %s"%(name, item2fill))
                setattr(item2fill, name, f[ext.name].data[name])
        #self.log.info(ret)
        return ret
    

    def __load_subdata_filter_astropy(self, ext, name, parent):
        self.log.info("%s - %s"%(ext, name))
        if not 'att' in self._def[ext][name] or \
           not 'group' in self._def[ext][name]['att']:
            return parent
        group = self._def[ext][name]['att']['group']
        self.log.info("Key %s is in the group %s"%(name, group))
        if group == ext:
            self.log.info("Group %s in %s"%(name, ext))
            if hasattr(parent, name):
                return getattr(parent, name)
            it = type("ExtensionItem", (Sc,),{})()
            setattr(parent, name, it)
            return it
        if group in self._def[ext]:
            gg = self._def[ext][group]
            if 'att' in gg and 'group' in gg['att']:
                parent = self.__load_subdata_filter_astropy(ext, group, parent)
            self.log.info("Creating group %s in %s"%(name, group))
            if hasattr(parent, name):
                return getattr(parent, name)
            it = type("ExtensionItem", (Sc,),{})()
            setattr(parent, name, it)
            return it
        return parent

    def create_r1(self, fits_file, fname = None,v='v1'):
        if not fits_file and not self._f:
            self.log.error("No FITS file provided")
            return False
    
        if fits_file and not self._f:
            self._f = fits.open(fits_file)
        elif fits_file and self._f:
            self._f.close()
            self._f = fits.open(fits_file)
        
        hdul = self._f
        self._h = h5_writer(fname)
        self._def = warehouse().get().fits_def
        for ext in hdul:
            if self._def and not ext.name in self._def:
                # FIXME: ?
                continue
            if hdul[ext.name].size <= 0:
                continue
            g = self._h.create_group(ext.name)
            header = g.create_group("header")
            for k in hdul[ext.name].header:
                hg = self._h.create_group(k, header)
                self._h.create_dataset("comment",hdul[ext.name].header.comments[k], hg)
                self._h.create_dataset("value",hdul[ext.name].header[k], hg)

            data = g.create_group("data")
            for name in hdul[ext.name].columns.names:
                group2fill = data
                if not self._def or \
                   (self._def and not name in self._def[ext.name]):
                    self.log.info("Key not found in the defintion file")
                else:
                    if 'att' in self._def[ext.name][name] and \
                       'group' in self._def[ext.name][name]['att']:
                        group = self._def[ext.name][name]['att']['group']
                        group2fill = self.__create_group(ext.name, group, group2fill)
                    else:
                        self.log.info("No group attribute found in key %s"%(name))
                self.log.info("Creating dataset %s in %s"%(name, group2fill.name))
                
                if v == 'v2':
                    cdata = hdul[ext.name].data[name]
                    if type(cdata) is fits.column._VLF:
                        # FIXME: What if it is not?
                        if cdata.size > 1:
                            # FIXME: Assuming all fields are ndarrays with the same dtype
                            dt = self._h.create_special_dtype(np.dtype(cdata[0].dtype))
                            ds = self._h.create_multidataset(name,(cdata.size,), 
                                                             dt, 
                                                             group2fill)
                            i = 0
                            for d in cdata:
                                ds[i] = d
                                i = i + 1
                        else:
                            if type(cdata[0]) is np.ndarray:
                                # FIXME: What if it is not?
                                self._h.create_dataset(name, cdata[0], group2fill, cdata[0].dtype)
                elif v == 'v1':
                    i = 0
                    group2fill = self._h.create_group(name, group2fill)
                    for d in hdul[ext.name].data[name]:
                        # FIXME: Assuming all fields are ndarrays
                        self._h.create_dataset("%s_%d"%(name, i), d, group2fill, d.dtype)
                        i = i + 1
        return True
                         
    def __create_group(self, ext, name, parent):
        if not 'att' in self._def[ext][name] or \
           not 'group' in self._def[ext][name]['att']:
            return parent
        group = self._def[ext][name]['att']['group']
        self.log.info("Key %s is in the group %s"%(name, group))
        if group == ext:
            self.log.info("Creating group %s in %s"%(name, ext))
            return self._h.create_group(name, parent)
        if group in self._def[ext]:
            gg = self._def[ext][group]
            if 'att' in gg and 'group' in gg['att']:
                parent = self.__create_group(ext, group, parent)
            self.log.info("Creating group %s in %s"%(name, group))
            return self._h.create_group(name, parent)
        return parent
    
    def __init__(self, fname = None):
        if fname:
            self._f = fits.open(fname)
        self.log = logger().get_log("from_fits")
        self._def = warehouse().get().fits_def

    def __del__(self):
        if self._f:
            self._f.close()

    _f = None
    _h = None
    _def = None
    _fdata = None # Fits data
    log = None
