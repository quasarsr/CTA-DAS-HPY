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

@package hpy.h5base

--------------------------------------------------------------------------------

This module provides the base interface
"""
from abc import ABCMeta, abstractmethod

class h5writerbase(metaclass=ABCMeta):
    @abstractmethod
    def create_group(self, gname, parent):
        pass
    @abstractmethod
    def create_dataset(self, dsname, data, parent, dtype):
        pass
    @abstractmethod
    def create_multidataset(self, dsname, size, dtype, parent=None):
        pass
    @abstractmethod
    def create_special_dtype(self, vlen):
        pass
    
class h5readerbase(metaclass=ABCMeta):
    @abstractmethod
    def get_dataset(self, dname, parent):
        pass
    @abstractmethod
    def get_group(self, gname, parent):
        pass
    
