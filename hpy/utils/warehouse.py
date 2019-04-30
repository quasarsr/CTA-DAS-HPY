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

@package hpy.warehouse

--------------------------------------------------------------------------------

This module provides the warehouses for the application
"""

from .config_manager import configuration_manager

from hpy.log import logger

class warehouse:
    """@class warehouse
    This class provides the global configuration constants

    It uses the singleton pattern to ensure there is only one instance of the 
    warehouse and also to provide accessibility in any section of the code
    """
    class __wh:
        """Private class to feature the singleton pattern"""
        ## Name
        NAME = "hpy"
        ## Version
        VERSION = "0.0.1"
        ## FITS file's definition
        fits_def = None
        ## Log
        log = None
        def load(self, cfg = None, ftype='fits'):
            """Loads the warehouse

            Loads the warehouse with the given configuration

            Args:
            self: The object pointer
            cfg: The configuration to be loaded
            """
            if not cfg:
                self.log.warning('cfg missing, loading default configuration...')
                return
            if ftype == 'fits':
                c = configuration_manager()
                if not c.load(cfg):
                    self.log.warning('Configuration file missing, loading default configuration...')
                    return
                self.fits_def = {}
                for ext in c.get_root():
                    self.fits_def[ext.tag] = c.get(ext.tag)
            
        def __init__(self, config_file = None):
            """Constructor
            Initializes the log and sets some the initial values of the 
            warehouse
            
            Args:
            self: The object pointer
            """
            self.log = logger().get_log("warehouse")
            
            c = configuration_manager()
            if not config_file: config_file = "conf/config.xml"
            if c.load(config_file):
                cfg = c.get("global")
                if cfg:
                    if 'name' in cfg:
                        self.NAME = cfg['name']
                    if 'version' in cfg:
                        self.VERSION = cfg['version']
        #
        #
        #
    def __init__(self, config_file = None):
        """Constructor
        
        The constructor will create a new __wh object in case 
        there is none initialized, otherwise it will not do anything
        
        Args:
        self: The object pointer
        """
        if not warehouse.instance:
            warehouse.instance = warehouse.__wh(config_file)

    def get(self):
        """Returns the warehouse intance
        
        Args:
        self: The object pointer
        
        Returns:
        The instance of the warehouse
        """
        return warehouse.instance

    instance = None
