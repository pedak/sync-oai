#!/usr/bin/env python
# encoding: utf-8
"""
simulate-source: The ResourceSync command line tool for simulating a changing
Web data source.

Created by Bernhard Haslhofer on 2012-04-24.
Copyright 2012, ResourceSync.org. All rights reserved.
"""

import argparse
import yaml
import logging
import logging.config

from resync.source import Source
from resync.http import HTTPInterface

DEFAULT_CONFIG_FILE = 'config/default.yaml'
DEFAULT_LOG_FILE = 'config/logging.yaml'

def main():
    
    # Define simulator options
    parser = argparse.ArgumentParser(
                            description = "ResourceSync Source Simulator")
    parser.add_argument('--config-file', '-c', 
                    default=DEFAULT_CONFIG_FILE,
                    help="the simulation configuration file")
    parser.add_argument('--log-config', '-l',
                    default=DEFAULT_LOG_FILE,
                    help="the logging configuration file")
    parser.add_argument('--port', '-p', type=int,
                    default=8888,
                    help="the HTTP interface port")
    parser.add_argument('--hostname', '-n',
                    default="localhost",
                    help="the hostname where the simulator is running")
    
    # Parse command line arguments
    args = parser.parse_args()

    # Load the logging configuration file and set up logging
    logconfig = yaml.load(file(args.log_config, 'r'))
    logging.config.dictConfig(logconfig)
    
    # Load the YAML configuration file
    config = yaml.load(file(args.config_file, 'r'))
    
    # Set up the source
    source_settings = config['source']
    source = Source(source_settings, args.hostname, args.port)
    
    # Set up and register the source inventory (if defined)
    if config.has_key('inventory_builder'):
        klass_name = config['inventory_builder']['class']
        mod = __import__('resync.source', fromlist=[klass_name])
        inventory_builder_klass = getattr(mod, klass_name)
        builder = inventory_builder_klass(source, config['inventory_builder'])
        source.add_inventory_builder(builder)
    
    # Set up and register change memory (if defined)
    if config.has_key('changememory'):
        klass_name = config['changememory']['class']
        mod = __import__('resync.changememory', fromlist=[klass_name])
        changemem_klass = getattr(mod, klass_name)


        changememory = changemem_klass(source, config['changememory'])
        source.add_changememory(changememory)
    
    # Set up and register publishers (if defined)
    if config.has_key('publisher'):
        klass_name = config['publisher']['class']
        mod = __import__('resync.publisher', fromlist=[klass_name])
        publisher_klass = getattr(mod, klass_name)
        publisher = publisher_klass(source, config['publisher'])
    
    # Attach event loggers;
    if config.has_key('logger'):
        klass_name = config['logger']['class']
        mod = __import__('resync.event_log', fromlist=[klass_name])
        logger_class = getattr(mod, klass_name)
        logger = logger_class(source, config['logger'])
    
    # Bootstrap the source
    source.bootstrap()
    
    # Start the Web interface, run the simulation
    # Attach HTTP interface to source
    http_interface = HTTPInterface(source)
    try:
        DEFAULT_OAI_ENDPOINT = 'http://eprints.ucm.es/cgi/oai2'
        DEFAULT_OAI_ENDPOINT = "http://eprints.erpanet.org/perl/oai2"
        http_interface.start()
        source.bootstrap_oai(DEFAULT_OAI_ENDPOINT)

    except KeyboardInterrupt:
        print "Exiting gracefully..."
    finally:
        http_interface.stop()

if __name__ == '__main__':
    main()