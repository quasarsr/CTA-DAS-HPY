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

@package hpy.h5

--------------------------------------------------------------------------------

This module provides the HDF5 interface
"""

import h5py
import numpy as np

from hpy.log import logger
from hpy.core.h5base import h5writerbase, h5readerbase

COMPRESSION_TYPES = ["gzip", "lzf", "szip"]

class h5_writer(h5writerbase):

    def create_group(self, gname, parent=None):
        if not parent: parent = self._f
        if not gname in parent.keys():
            self.log.info("Creating group %s in %s"%(gname, parent.name))
            return parent.create_group(gname)
        return parent[gname]

    def create_dataset(self, dsname, data, parent=None, dtype=None, 
                       maxshape=None, chunks=None):
        if not parent: parent = self._f
        if chunks is not None:
            if not maxshape:
                maxshape=(None, None)

            if dtype:
                self.log.info("Creating dataset %s in %s dtype %s"%(dsname, parent.name, dtype))
                if self._compression and isinstance(data, np.ndarray):
                    if self._compression == COMPRESSION_TYPES[0]:
                        self.log.info("Creating dataset %s in %s with compression %s dtype %s"%(dsname, parent.name, self._compression,dtype))
                        return parent.create_dataset(dsname, data=data, 
                                                     compression = self._compression, 
                                                     compression_opts = self._compression_opts,
                                                     dtype=dtype,
                                                     maxshape=maxshape,
                                                     chunks=chunks)
                    self.log.info("Creating dataset %s in %s with compression %s dtype %s"%(dsname, parent.name, self._compression,dtype))
                    return parent.create_dataset(dsname, data=data, 
                                                 compression = self._compression, 
                                                 dtype=dtype,
                                                 maxshape=maxshape,
                                                 chunks=chunks)

                    
                return parent.create_dataset(dsname, data=data, dtype=dtype,
                                             maxshape=maxshape,
                                             chunks=chunks)
        
            if self._compression and isinstance(data, np.ndarray):
                if self._compression == COMPRESSION_TYPES[0]:
                    self.log.info("Creating dataset %s in %s with compression %s"%(dsname, parent.name, self._compression))
                    return parent.create_dataset(dsname, data=data, 
                                                 compression = self._compression, 
                                                 compression_opts = self._compression_opts,
                                                 maxshape=maxshape,
                                                 chunks=chunks)
                self.log.info("Creating dataset %s in %s with compression %s"%(dsname, parent.name, self._compression))
                return parent.create_dataset(dsname, data=data, 
                                             compression = self._compression,
                                             maxshape=maxshape,
                                             chunks=chunks)
            self.log.info("Creating dataset %s in %s"%(dsname, parent.name))
            return parent.create_dataset(dsname, data=data,
                                         maxshape=maxshape,
                                         chunks=chunks)
        # No chunking
        if dtype:
            self.log.info("Creating dataset %s in %s dtype %s"%(dsname, parent.name, dtype))
            if self._compression and isinstance(data, np.ndarray):
                if self._compression == COMPRESSION_TYPES[0]:
                    self.log.info("Creating dataset %s in %s with compression %s dtype %s"%(dsname, parent.name, self._compression,dtype))
                    return parent.create_dataset(dsname, data=data, 
                                                 compression = self._compression, 
                                                 compression_opts = self._compression_opts,
                                                 dtype=dtype)
                self.log.info("Creating dataset %s in %s with compression %s dtype %s"%(dsname, parent.name, self._compression,dtype))
                return parent.create_dataset(dsname, data=data, 
                                             compression = self._compression, 
                                             dtype=dtype)

                    
            return parent.create_dataset(dsname, data=data, dtype=dtype)
        
        if self._compression and isinstance(data, np.ndarray):
            if self._compression == COMPRESSION_TYPES[0]:
                self.log.info("Creating dataset %s in %s with compression %s"%(dsname, parent.name, self._compression))
                return parent.create_dataset(dsname, data=data, 
                                             compression = self._compression, 
                                             compression_opts = self._compression_opts)
            self.log.info("Creating dataset %s in %s with compression %s"%(dsname, parent.name, self._compression))
            return parent.create_dataset(dsname, data=data, 
                                         compression = self._compression)
        self.log.info("Creating dataset %s in %s"%(dsname, parent.name))
        return parent.create_dataset(dsname, data=data)
    
    def create_multidataset(self, dsname, size, dtype, parent=None):
        if not parent: parent = self._f
        self.log.info("Creating dataset %s in %s dtype %s"%(dsname, parent.name, dtype))
        return parent.create_dataset(dsname, size, dtype=dtype)

    def create_special_dtype(self, vlen):
        return h5py.special_dtype(vlen=vlen)
    
    def check_dataset(self, dname, parent = None):
        if not parent: return self.check_dataset(dname, self._f)
        if type(parent) != h5py._hl.group.Group and \
           type(parent) != h5py._hl.files.File: 
            return None
        ret = parent.get(dname)
        if not ret:
            for k in parent.keys():
                ret = self.check_dataset(dname, parent[k])
                if ret: return ret
            
            return None
        #ret_class = parent.get(gname, getclass=True)
        if type(ret) != h5py._hl.dataset.Dataset: return None 
        return ret
    
    def append_str(self, data, dset):
        old_len = dset.shape[1]
        dset.resize((1, dset.shape[1] + 1))
        dset[0 , old_len] = data.encode()

    def append_data(self, data, dset):
        if len(dset.shape) == 1:
            old_len = 1
            dset.resize((2, len(data)))
        else:
            old_len = dset.shape[0]
            dset.resize((dset.shape[0] + 1, len(data)))
        dset[old_len:] = data
        
    def __init__(self, fname=None, mode='w', 
                 compression=None, compression_opts=0, **kwargs):
        super().__init__()
        
        self.log = logger().get_log("h5")
        if not fname: fname = "d.h5"
        if mode not in ['a', 'w', 'r+']:
            self.log.error("Invalid mode")
            return
        if compression in COMPRESSION_TYPES:
            self._compression = compression
        self._compression_opts = compression_opts
        kwargs.update(mode=mode)
        self._f = h5py.File(fname, **kwargs)
    
    def close(self):
        self._f.close()
        self._f = None

    def __del__(self):
        if self._f:
            self._f.close()

    _f = None
    log = None
    _compression = None

class h5_reader(h5readerbase):

    def __get_dataset(self, dname, parent = None):
        if type(parent) != h5py._hl.group.Group and \
           type(parent) != h5py._hl.files.File: 
            return None
        ret = parent.get(dname)
        if not ret:
            for k in parent.keys():
                ret = self.__get_dataset(dname, parent[k])
                if ret: return ret
            
            return None
        #ret_class = parent.get(gname, getclass=True)
        if type(ret) != h5py._hl.dataset.Dataset: return None 
        return ret
    
    def get_dataset(self, dname, parent = None):
        if not parent: parent = self._f
        return self.__get_dataset(dname, parent)

    def __get_group(self, gname, parent = None):
        if type(parent) != h5py._hl.group.Group and \
           type(parent) != h5py._hl.files.File: 
            return None
        ret = parent.get(gname)
        if not ret:
            for k in parent.keys():
                ret = self.__get_group(gname, parent[k])
                if ret: return ret
            
            return None
        if type(ret) != h5py._hl.group.Group: return None
        return ret
        
    def get_group(self, gname, parent = None):
        if not parent: parent = self._f
        return self.__get_group(gname, parent)

    def __init__(self, fname=None, mode='r'):
        
        self.log = logger().get_log("h5")
        self._f = h5py.File(fname, mode)

    def close(self):
        self._f.close()
        self._f = None

    def __del__(self):
        if self._f:
            self._f.close()

    _f = None
    log = None
