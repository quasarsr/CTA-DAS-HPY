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

@package hpy.config_manager

--------------------------------------------------------------------------------

This module features the configuration_manager class
"""
import xml.etree.ElementTree
import os.path

from hpy.log import logger

class configuration_manager:
    """@class configuration_manager
    This class manages the application configuration

    The class reads the configuration files and store the information readed.
    """
    ## XML file's root
    xml_root = None
    ## The log
    log = None

    def __init__(self):
        """The constructor
        
        Initializes the log

        Args:
        self: The object pointer
        """
        self.log = logger().get_log('configuration_manager')

    def get(self, config_name):
        """Returns the given configuration
        
        Reads the xml_root and returns the given configuration from it.

        Args:
        self: The object pointer
        config_name: The configuration to be returned

        Returns: 
        The given configuration if it is located in the xml_root, None otherwise
        """
        if not self.xml_root:
            self.log.error("There is no config file loaded")
            return None
        ret = None
        for child in self.xml_root:
            if child.tag == config_name:
                ret = {}
                for att in child:
                    ret[att.tag] = { 'text': child.find(att.tag).text,
                                     'att': child.find(att.tag).attrib }
        return ret
    
    def get_config(self, config_name):
        """Returns the given configuration
        
        Reads the xml_root and returns the given configuration from it in a 
        dictionary.

        Args:
        self: The object pointer
        config_name: The configuration to be returned
        
        Returns:
        The given configuration if it is located in the xml_root, None otherwise
        """
        if not self.xml_root:
            self.log.error("There is no config file loaded")
            return None
        ret = None
        for child in self.xml_root:
            if child.tag == config_name:
                ret = self.__load_level(child)
                    
        return ret

    def __load_level(self, node):
        """Returns the configuration for a given xml node

        Reads the xml node and returns the configuration in a dictionary.
        
        If the node has childs it will call itself recursively to retrieve
        the configuration from those childs.

        Args:
        self: The object pointer
        node: The node to be read

        Returns:
        The configuration from the given xml node
        """
        if not node:
            self.log.error("No node provided")
            return None
        ret = {}
        for n in node:
            self.log.info(n.attrib)
            if len(list(n)):
                   ret[n.tag] = { 'node': self.__load_level(n),
                                  'att': n.attrib, 'text': n.text }
            else:
                ret[n.tag] = {'node' : None, 'att': n.attrib, 'text': n.text}
        return ret

    def get_root(self):
        """Returns the root element
        
        Args:
        self: The object pointer
        
        Returns:
        The root element
        """
        return self.xml_root

    def load(self, config_file):
        """Loads the configuration file
        
        Loads a given configuration file

        Args:
        self: The object pointer
        config_file: The configuration file
        
        Returns:
        True if the configuration file was read correctly, False otherwise
        """
        if(not os.path.isfile(config_file)):
            self.log.error('Configuration file %s not found!', config_file)
            return False
        self.xml_root = xml.etree.ElementTree.parse(config_file).getroot()
        return True
