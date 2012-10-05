#!/usr/bin/env python
# encoding: utf-8
"""
simulate-oai-adapter: The ResourceSync command line tool for OAI-PMH synchronization.

Created by Peter Kalchgruber on 2012-09-01.
Based on simulate-source of Bernhard Haslhofer
"""

import optparse
import yaml
import logging

from resync.http import HTTPInterface
from resync.util import UTCFormatter
from resync.wikisource import Source

DEFAULT_CONFIG_FILE = 'config/default.yaml'
SOURCE_LOG_FILE = 'resync-source.log'

def init_logging(file=False, console=False, eval_mode=False):
    """Initialize logging"""
    
    fmt = '%(asctime)s | %(name)s | %(levelname)s | %(message)s'
    formatter = UTCFormatter(fmt)
    
    if console:
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
    if file:
        fh = logging.FileHandler(filename=SOURCE_LOG_FILE, mode='a')
        fh.setFormatter(formatter)
    
    loggers = ['source']
    for logger in loggers:
        log = logging.getLogger(logger)
        if eval_mode:
            log.setLevel(logging.DEBUG)
        else:
            log.setLevel(logging.INFO)
        if console:
            log.addHandler(ch)
        if file:
            log.addHandler(fh)

def main():
    
    p = optparse.OptionParser(description='ResourceSync OAI-PMH adapter',
        usage='usage: %prog [options] uri_path local_path  (-h for help)')
        
    # Define adapter options
    p.add_option('--config-file', '-c', type=str, action='store',
                    default=DEFAULT_CONFIG_FILE,
                    help="the simulation configuration file")
    p.add_option('--endpoint-url', '-u', type=str, action='store',
                    help="the oai-pmh endpoint")
                    
    p.add_option('--port', '-p', type=int, action='store',
                    default=8888,
                    help="the HTTP interface port")
    p.add_option('--hostname', '-n', type=str, action='store',
                    default="localhost",
                    help="the hostname where the adapter is running")
    p.add_option('--logger', '-l', action='store_true',
                    default=False,
                    help="write detailed logs of source to file")
    p.add_option('--verbose', '-v', action='store_true',
                    default=False,
                    help="verbose")
    p.add_option('--eval', '-e', action='store_true',
                    default=False,
                    help="run in evaluation mode (log all events)")
    
    # Parse command line arguments
    (args, map) = p.parse_args()
    if(args.logger or args.verbose):
        init_logging(file=args.logger, console=args.verbose,
                     eval_mode=args.eval)
        
    # Load the YAML configuration file
    config = yaml.load(file(args.config_file, 'r'))
    
    print "Setting up ResourceSync OAI-Adapter..."
    
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
    print "Bootstrapping the source ..."
    source.bootstrap()
    print "OAI-PMH adapter is now running at %s" % source.base_uri
    
    # Start the Web interface, run the simulation
    # Attach HTTP interface to source
    http_interface = HTTPInterface(source)
    try:
        http_interface.start()
        source.bootstrap_irc(source_settings['endpoint'],source_settings['channel'])
    except KeyboardInterrupt:
        print "\nStopping irc adapter, server and exiting gracefully..."
    finally:
        source.disconnect()
        http_interface.stop()

if __name__ == '__main__':
    main()