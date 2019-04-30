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

@package hpy.table

--------------------------------------------------------------------------------

This module provides the pytables interface
"""
import re

import collections

from abc import ABCMeta, abstractmethod
from hpy.log import logger
from hpy.core.h5base import h5writerbase, h5readerbase
from hpy.utils.data_container import data_container

class table_writer(h5writerbase, metaclass=ABCMeta):
    @abstractmethod
    def create_group(self, gname, parent):
        pass
    @abstractmethod
    def create_dataset(self, dsname, data, parent=None, dtype=None):
        pass
    @abstractmethod
    def create_multidataset(self, dsname, size, dtype, parent=None):
        pass
    @abstractmethod    
    def create_special_dtype(self, vlen):
        pass
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._transforms = collections.defaultdict(dict)
        self._exclusions = collections.defaultdict(list)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    def exclude(self, table_name, pattern):
        self._exclusions[table_name].append(re.compile(pattern))

    def _is_column_exclude(self, table_name, col_name):
        for pattern in self._exclusions[table_name]:
            if pattern.match(col_name):
                return True
        return False

    def add_column_transform(self, table_name, col_name, transform):
        self._transform[table_name][col_name] = transform

    @abstractmethod
    def open(self, filename, **kwargs):
        pass
    @abstractmethod
    def close(self):
        pass

    def _apply_col_transform(self, table_name, col_name, value):
        if col_name in self._transforms[table_name]:
            tr = self._transform[table_name][col_name]
            value = tr(value)
        return value

class table_reader(h5readerbase, metaclass=ABCMeta):
    @abstractmethod
    def get_dataset(self, dname, parent):
        pass
    @abstractmethod
    def get_group(self, gname, parent):
        pass

    def __init__(self):
        super().__init__()
        self._cols_to_read = collections.defaultdict(list)
        self._transforms = collections.defaultdict(dict)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def add_column_transform(self, table_name, col_name, transform):
        self._transforms[table_name][col_name] = transform

    def _apply_col_transform(self, table_name, col_name, value):
        if col_name in self._transforms[table_name]:
            tr = self._transforms[table_name][col_name]
            value = tr(value)
        return value
