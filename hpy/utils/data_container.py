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

@package hpy.dataset

--------------------------------------------------------------------------------

This module provides a wrapper for the containers
"""
try:
    from ctapipe.core import Container as Cnt
    from ctapipe.core import Field as Field
except ImportError:
    from hpy.utils.container import Container as Cnt
    from hpy.utils.container import Field as Field

class schema:
    def __str__(self):
        return str(self.__dict__)

class data_container(Cnt):
    pass
"""
    def __init__(self):
        self.defattr('test', 1)
    def defattr(self, attr, value):
        self.__dict__.update({attr: value})
"""
