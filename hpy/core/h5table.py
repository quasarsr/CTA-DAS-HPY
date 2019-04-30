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

@package hpy.h5table

--------------------------------------------------------------------------------

This module provides the pytables interface
"""
import tables

import numpy as np

from hpy.log import logger
from hpy.core.table import table_writer, table_reader
from hpy.utils.data_container import data_container

PYTABLES_TYPE_MAP = {
    'float': tables.Float64Col,
    'float64': tables.Float64Col,
    'float32': tables.Float32Col,
    'int': tables.IntCol,
    'int8': tables.Int8Col,
    'int16': tables.Int16Col,
    'int32': tables.Int32Col,
    'int64': tables.Int64Col,
    'uint': tables.UIntCol,
    'uint8': tables.UInt8Col,
    'uint16': tables.UInt16Col,
    'uint32': tables.UInt32Col,
    'uint64': tables.UInt64Col,
    'bool': tables.BoolCol,
    'str': tables.StringCol,
}

COMPRESSION_FILTERS_TYPES = ['zlib', 'lzo', 'bzip2', 'blosc']

class h5table_writer(table_writer):

    def create_group(self, gname, parent = None):
        if not parent: parent = self._f.root
        if not gname in parent:
            self.log.info("Creating group %s in %s"%(gname, parent._v_name))
            return self._f.create_group(parent, gname)
        return getattr(parent, gname)

    def create_dataset(self, dsname, data, parent=None, dtype=None):
        if not parent: parent = self._f.root
        datasets = self.__create_dataset(data)
        
        self.log.info(datasets)

        if isinstance(datasets, data_container):
            datasets = (datasets,)
        
        table = self.__check_table(dsname, parent)
        if not table:
            table = self.__create_new_table(dsname, parent, datasets)

        ret = self.__append_row(table, datasets)

        return ret

        dclass = type("data", (dataset,), {"d1": field(None)})
        d = dclass()
        d.d1 = data
        print(d.d1)
        schema = self.__create_schema(dsname, dclass)
        table = self._f.create_table(parent, dsname, schema)
        row = table.row
        row['d1'] = data
        row.append()
        table.flush()

    def create_multidataset(self, dsname, size, dtype, parent=None):
        pass
    def create_special_dtype(self, vlen):
        pass

    def check_dataset(self, dname, parent = None):
        if not parent: return self.check_dataset(dname, self._f.root)
        if not isinstance(parent, tables.table.Table) and \
           not isinstance(parent, tables.group.RootGroup) and \
           not isinstance(parent, tables.group.Group):
            return None
        if parent._v_name == dname and \
           (isinstance(parent, tables.table.Table)): 
            return parent
        for k in parent:
            ret = self.check_dataset(dname, k)
            if ret: return ret
        return None 

    def append_data(self, datasets, table):
        row = table.row
        for dataset in datasets:
            for colname in filter(lambda c:c in table.colnames,
                                  dataset.keys()):
                value = self._apply_col_transform(
                    table._v_name, colname, dataset[colname])
                row[colname] = value
        row.append()
        table.flush()

    def __create_dataset(self, data):
        # TODO:
        return data
        dclass = type("dclass", (dataset, ), {"d1": field(None)})
        ret = dclass()
        ret.d1 = data
        return ret

    def __append_row(self, table_name, datasets):
        table = self._tables[table_name]
        row = table.row
        for dataset in datasets:
            for colname in filter(lambda c:c in table.colnames,
                                  dataset.keys()):
                value = self._apply_col_transform(
                    table_name, colname, dataset[colname])
                row[colname] = value
        row.append()
        table.flush()
        return 
        return row

    def __check_table(self, dsname, parent):
        join = ""
        if parent._v_pathname != "/":
            join = "/"
        table_name = parent._v_pathname + join + dsname
        if not table_name in self._schemas:
            return None
        return table_name
    
    def __create_new_table(self, dsname, parent, datasets):
        join = ""
        if parent._v_pathname != "/":
            join = "/"
        table_name = parent._v_pathname + join + dsname
        meta = self.__create_schema(table_name, datasets)

        for dataset in datasets:
            meta.update(dataset.meta)

        self.log.info(table_name)
        self.log.info(parent)

        table = self._f.create_table(where=parent,name=dsname,
                                     title="Storage of {}".format(
                                         ",".join(c.__class__.__name__ for c in datasets)),
                                     description=self._schemas[table_name])
        for k, v in meta.items():
            table.attrs[k] = v
        self._tables[table_name] = table
        
        return table_name

    def __create_schema(self, table_name, datasets):
        class cschema(tables.IsDescription):
            pass
            
        meta = {}

        for ds in datasets:
            for col_name, v in ds.items():
                typename = ""
                shape = 1
                if self._is_column_exclude(table_name, col_name):
                    continue
                if type(v).__name__ == "_VLF":
                    i = 0
                    for d in v:
                        typename = d.dtype.name
                        coltype = PYTABLES_TYPE_MAP[typename]
                        shape = d.shape
                        cschema.columns["%s_%d"%(col_name,i)] = coltype(shape=shape)
                        i = i + 1
                elif isinstance(v, np.ndarray):
                    typename = v.dtype.name
                    coltype = PYTABLES_TYPE_MAP[typename]
                    shape = v.shape
                    cschema.columns[col_name] = coltype(shape=shape)
                elif isinstance(v, str):
                    typename = type(v).__name__
                    coltype = PYTABLES_TYPE_MAP[typename]
                    size = len(v)
                    if size == 0: size = 1
                    cschema.columns[col_name] = coltype(size)
                elif type(v).__name__ in PYTABLES_TYPE_MAP:
                    typename = type(v).__name__
                    coltype = PYTABLES_TYPE_MAP[typename]
                    cschema.columns[col_name] = coltype()
 
        self._schemas[table_name] = cschema
        return meta

    def __create_compression_filter(self, compression, compression_opts):
        return tables.Filters(complib=compression, complevel=compression_opts)

    def __init__(self, filename="d.h5", mode='w', 
                 compression = None, compression_opts=0, **kwargs):
        super().__init__()
        self._schemas = {}
        self._tables = {}
        
        if not filename: filename = "d.h5"

        self.log = logger().get_log("table_writer")
        if mode not in ['a', 'w', 'r+']:
            return
        kwargs.update(mode=mode)
        self.open(filename, compression, compression_opts, **kwargs)

    def open(self, filename, compression = None, compression_opts=0, **kwargs):
        if compression in COMPRESSION_FILTERS_TYPES:
            self._f = tables.open_file(filename, 
                                       filters=self.__create_compression_filter(compression, compression_opts),
                                       **kwargs)
        else:
            self._f = tables.open_file(filename,**kwargs)

    def close(self):
        self._f.close()
        self._f = None

    def __del__(self):
        if self._f:
            self.close()

    _f = None
    log = None

class h5table_reader(table_reader):
    def get_dataset(self, dname, parent = None):
        if not parent: parent = self._f.root
        return self.__get_dataset(dname, parent)

        join = ""
        if parent._v_pathname != "/":
            join = "/"
        table_name = parent._v_pathname + join + dsname
        
        ret = None

        if table_name not in self._tables:
            table = self._setup_table(table_name, ret)
        else:
            table = self._table[table_name]
    
    def __get_dataset(self, dname, parent):
        if not isinstance(parent, tables.table.Table) and \
           not isinstance(parent, tables.group.RootGroup) and \
           not isinstance(parent, tables.group.Group):
            return None
        if parent._v_name == dname and \
           (isinstance(parent, tables.table.Table)): 
            return parent
        for k in parent:
            ret = self.__get_dataset(dname, k)
            if ret: return ret
        return None 
        

    def get_group(self, gname, parent = None):
        if not parent: parent = self._f.root
        return self.__get_group(gname, parent)

    def __get_group(self, gname, parent):
        if not isinstance(parent, tables.group.RootGroup) and \
           not isinstance(parent, tables.group.Group):
            return None
        if parent._v_name == gname and \
           (isinstance(parent, tables.group.RootGroup) or \
            isinstance(parent, tables.group.Group)): 
            return parent
        for k in parent:
            ret = self.__get_group(gname, k)
            if ret: return ret
        return None
                
            

    def __init__(self, filename, **kwargs):
        self.log = logger().get_log("table_reader")
        super().__init__()
        self._tables = {}
        kwargs.update(mode='r')
        self.open(filename, **kwargs)

    def open(self, filename, **kwargs):
        self._f = tables.open_file(filename, **kwargs)

    def close(self):
        self._f.close()
        self._f = None

    def __del__(self):
        if self._f:
            self.close()
            
    _f = None
    log = None
