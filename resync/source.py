#!/usr/bin/env python
# encoding: utf-8
"""
source.py: A source holds a set of resources and changes over time.

Resources are internally stored by their basename (e.g., 1) for memory
efficiency reasons.

Created by Bernhard Haslhofer on 2012-04-24.
Adopted by Peter Kalchgruber on 2012-09-01.
Copyright 2012, ResourceSync.org. All rights reserved.
"""

import re
import os
import random
import pprint
import logging
import time
import shutil

import tornado.ioloop
import tornado.web

from apscheduler.scheduler import Scheduler

from resync.observer import Observable
from resync.resource_change import ResourceChange
from resync.resource import Resource
from resync.digest import compute_md5_for_string, compute_md5_for_url
from resync.inventory import Inventory
from resync.sitemap import Sitemap, Mapper

##oai
from pyoaipmh.pyoai import Client, Header, Record, NoRecordsException
from pyoaipmh.common import Common
import datetime
from urllib2 import URLError
from dateutil import parser as dateutil_parser
import re
#### Source-specific capability implementations ####

class DynamicInventoryBuilder(object):
    """Generates an inventory snapshot from a source"""
    
    def __init__(self, source, config):
        self.source = source
        self.config = config
        self.logger = logging.getLogger('inventory_builder')
        
    def bootstrap(self):
        """Bootstrapping procedures implemented in subclasses"""
        pass
    
    @property
    def path(self):
        """The inventory path (from the config file)"""
        return self.config['uri_path']

    @property
    def uri(self):
        """The inventory URI (e.g., http://localhost:8080/sitemap.xml)"""
        return self.source.base_uri + "/" + self.path
    
    def generate(self):
        """Generates an inventory (snapshot from the source)"""
        then = time.time()
        self.logger.debug("Start inventory generation")
        capabilities = {}
        if self.source.has_changememory:
            next_changeset = self.source.changememory.next_changeset_uri()
            capabilities[next_changeset] = {"type": "changeset"}
        inventory = Inventory(resources=self.source.resources,
                              capabilities=capabilities)
        now = time.time()
        self.logger.info("Generated inventory: %f" % (now-then))
        return inventory
        
class StaticInventoryBuilder(DynamicInventoryBuilder):
    """Periodically writes an inventory to the file system"""
    
    def __init__(self, source, config):
        super(StaticInventoryBuilder, self).__init__(source, config)
                                
    def bootstrap(self):
        """Bootstraps the static inventory writer background job"""
        self.rm_sitemap_files(Source.STATIC_FILE_PATH)
        self.write_static_inventory()
        logging.basicConfig()
        interval = self.config['interval']
        sched = Scheduler()
        sched.start()
        sched.add_interval_job(self.write_static_inventory,
                                seconds=interval)
    
    def generate(self):
        """Generates an inventory (snapshot from the source)
        TODO: remove as soon as resource container _len_ is fixed"""
        capabilities = {}
        if self.source.has_changememory:
            next_changeset = self.source.changememory.next_changeset_uri()
            capabilities[next_changeset] = {"type": "changeset"}
        # inventory = Inventory(resources=self.source.resources,
        #                       capabilities=capabilities)
        inventory = Inventory(resources=None, capabilities=capabilities)
        for resource in self.source.resources:
            if resource is not None: inventory.add(resource)
        return inventory
    
    def write_static_inventory(self):
        """Writes the inventory to the filesystem"""
        # Log Sitemap create start event
        self.logger.info("Start writing Sitemap inventory.")
        sm_write_start = ResourceChange(
                resource = ResourceChange(self.uri, 
                                timestamp=time.time()),
                                changetype = "SITEMAP UPDATE START")
        self.source.notify_observers(sm_write_start)
        
        # Generate sitemap in temp directory
        then = time.time()
        self.ensure_temp_dir(Source.TEMP_FILE_PATH)
        inventory = self.generate()
        basename = Source.TEMP_FILE_PATH + "/sitemap.xml"
        s=Sitemap()
        s.max_sitemap_entries=self.config['max_sitemap_entries']
        s.mapper=Mapper([self.source.base_uri, Source.TEMP_FILE_PATH])
        s.write(inventory, basename)
        # Delete old sitemap files; move the new ones; delete the temp dir
        self.rm_sitemap_files(Source.STATIC_FILE_PATH)
        self.mv_sitemap_files(Source.TEMP_FILE_PATH, Source.STATIC_FILE_PATH)
        shutil.rmtree(Source.TEMP_FILE_PATH)
        now = time.time()
        # Log Sitemap create start event
        sitemap_size = self.compute_sitemap_size(Source.STATIC_FILE_PATH)
        log_data = {'time': (now-then), 
                    'no_resources': self.source.resource_count}
        self.logger.info("Wrote static sitemap inventory. %s" % log_data)
        sm_write_end = ResourceChange(
                resource = ResourceChange(self.uri, 
                                size=sitemap_size,
                                timestamp=then),
                                changetype = "UPDATED")
        self.source.notify_observers(sm_write_end)
        
    def ensure_temp_dir(self, temp_dir):
        """Create temp directory if it doesn't exist; removes existing one"""
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            os.makedirs(temp_dir)
        else:
            os.makedirs(temp_dir)
    
    def ls_sitemap_files(self, directory):
        """Returns the list of sitemaps in a directory"""
        p = re.compile('sitemap\d*\.xml')
        filelist = [ f for f in os.listdir(directory) if p.match(f) ]
        return filelist
    
    def rm_sitemap_files(self, directory):
        """Deletes sitemap files (from previous runs)"""
        filelist = self.ls_sitemap_files(directory)
        if len(filelist) > 0:
            self.logger.debug("*** Cleaning up %d sitemap files ***" % 
                                                                len(filelist))
            for f in filelist:
                filepath = directory + "/" + f
                os.remove(filepath)
    
    def mv_sitemap_files(self, src_directory, dst_directory):
        """Moves sitemaps from src to dst directory"""
        filelist = self.ls_sitemap_files(src_directory)
        if len(filelist) > 0:
            self.logger.debug("*** Moving %d sitemap files ***" % 
                                                                len(filelist))
            for f in filelist:
                filepath = src_directory + "/" + f
                shutil.move(filepath, dst_directory)
    
    def compute_sitemap_size(self, directory):
        """Computes the size of all sitemap files in a given directory"""
        return sum([os.stat(directory + "/" + f).st_size 
                        for f in self.ls_sitemap_files(directory)])
    
#### Source Simulator ####

class Source(Observable):
    """A source contains a list of resources and changes over time"""
    
    RESOURCE_PATH = "/resources"
    STATIC_FILE_PATH = os.path.join(os.path.dirname(__file__), "static")
    TEMP_FILE_PATH = os.path.join(os.path.dirname(__file__), "temp")
    
    def __init__(self, config, hostname, port):
        """Initalize the source"""
        super(Source, self).__init__()
        self.logger = logging.getLogger('source')
        self.config = config
        self.hostname = hostname
        self.port = port
        self.max_res_id = 1
        self._repository = {} # {basename, {timestamp, size}}
        self.inventory_builder = None # The inventory builder implementation
        self.changememory = None # The change memory implementation
        self.no_events = 0
        self.oaimapping = {} #oai
        self.client=None #oai
        self.lastcheckdate=dateutil_parser.parse(config['fromdate'].strftime("%Y-%m-%d %H:%SZ")) #oai
    
    ##### Source capabilities #####
    
    def add_inventory_builder(self, inventory_builder):
        """Adds an inventory builder implementation"""
        self.inventory_builder = inventory_builder
        
    @property
    def has_inventory_builder(self):
        """Returns True in the Source has an inventory builder"""
        return bool(self.inventory_builder is not None)        
    
    def add_changememory(self, changememory):
        """Adds a changememory implementation"""
        self.changememory = changememory
        
    @property
    def has_changememory(self):
        """Returns True if a source maintains a change memory"""
        return bool(self.changememory is not None)
    
    ##### Bootstrap Source ######

    def bootstrap(self):
        """Bootstrap the source with a set of resources"""
        self.logger.info("Bootstrapping source")
        
        if self.has_changememory: self.changememory.bootstrap()
        if self.has_inventory_builder: self.inventory_builder.bootstrap()
        self._log_stats()
    
    ##### Source data accessors #####
    
    @property
    def base_uri(self):
        """Returns the base URI of the source (e.g., http://localhost:8888)"""
        return "http://" + self.hostname + ":" + str(self.port)

    @property
    def resource_count(self):
        """The number of resources in the source's repository"""
        return len(self._repository)
    
    @property
    def resources(self):
        """Iterates over resources and yields resource objects"""
        for basename in self._repository.keys():
            resource = self.resource(basename)
            if resource is None:
                self.logger.error("Cannot create resource %s " % basename + \
                      "because source object has been deleted.")
            else:
                yield resource
    
    @property
    def random_resource(self):
        """Returns a single random resource"""
        rand_res = self.random_resources()
        if len(rand_res) == 1:
            return rand_res[0]
        else:
            return None
    
    def resource(self, basename):
        """Creates and returns a resource object from internal resource
        repository. Repositoy values are copied into the object."""
        if not self._repository.has_key(basename): return None
        uri = basename #oai
#        uri = self.base_uri + Source.RESOURCE_PATH + "/" + basename #oai
        timestamp = self._repository[basename]['timestamp']
        size = self._repository[basename]['size']
        md5=-1
        #DEBUG how to calculate md5
        #if size>0:
            #md5 = compute_md5_for_url(basename) #oai (will be called on every sitemap generation)
        return Resource(uri = uri, timestamp = timestamp, size = size,
                        md5 = md5)
    
    def resource_payload(self, basename, size = None):
        """Generates dummy payload by repeating res_id x size times"""
        if size == None: size = self._repository[basename]['size']
        no_repetitions = size / len(basename)
        content = "".join([basename for x in range(no_repetitions)])
        no_fill_chars = size % len(basename)
        fillchars = "".join(["x" for x in range(no_fill_chars)])
        return content + fillchars
    
    def random_resources(self, number = 1):
        "Return a random set of resources, at most all resources"
        if number > len(self._repository):
            number = len(self._repository)
        rand_basenames = random.sample(self._repository.keys(), number)
        return [self.resource(basename) for basename in rand_basenames]
    
    def simulate_changes(self):
        """Simulate changing resources in the source"""
        self.logger.info("Starting simulation...")
        sleep_time = self.config['change_delay']
        while self.no_events != self.config['max_events']:
            time.sleep(sleep_time)
            event_type = random.choice(self.config['event_types'])
            if event_type == "create":
                self._create_resource()
            elif event_type == "update" or event_type == "delete":
                if len(self._repository.keys()) > 0:
                    basename = random.sample(self._repository.keys(), 1)[0]
                else:
                    basename = None
                if basename is None: 
                    self.no_events = self.no_events + 1                    
                    continue
                if event_type == "update":
                    self._update_resource(basename)
                elif event_type == "delete":
                    self._delete_resource(basename)

            else:
                self.logger.error("Event type %s is not supported" 
                                                                % event_type)
            self.no_events = self.no_events + 1
            if self.no_events%self.config['stats_interval'] == 0:
                self._log_stats()

        self.logger.info("Finished change simulation")
    
    # Private Methods
    
    def _create_resource(self, basename = None, identifier = None, timestamp=time.time(), notify_observers = True):
        """Create a new resource, add it to the source, notify observers."""
        #size=Common.get_size(basename) -> causes block on bigger sites DEBUG
        size=-1
        self._repository[basename] = {'timestamp': timestamp, 'size': size}
        change = ResourceChange(resource = self.resource(basename),
                                changetype = "CREATED")
        self.oaimapping[identifier]=basename;                        
        if notify_observers:
            self.notify_observers(change)
            self.logger.debug("Event: %s" % repr(change))
        return change
        
    def _update_resource(self, basename, timestamp):
        """Update a resource, notify observers."""
        # size=Common.get_size(basename) -> causes block on bigger sites DEBUG
        size=-1
        self._repository[basename] = {'timestamp': timestamp, 'size': size}
        change = ResourceChange(
                    resource = self.resource(basename),
                    changetype = "UPDATED")
        self.notify_observers(change)
        self.logger.debug("Event: %s" % repr(change))

    def _delete_resource(self, identifier, timestamp, notify_observers = True):
        """Delete a given resource, notify observers."""
        basename=self.oaimapping[identifier]
        res = self.resource(basename)
        del self._repository[basename]
        del self.oaimapping[identifier]
        res.timestamp = timestamp
        if notify_observers:
            change = ResourceChange(resource = res, changetype = "DELETED")
            self.notify_observers(change)
            self.logger.debug("Event: %s" % repr(change))
    
    def bootstrap_oai(self,endpoint,startdate=None): #todo update granularity
        """bootstraps OAI-PMH Source"""
        if startdate is None:
            startdate=self.config['fromdate']
        self.logger.debug("Connection to OAI-Endpoint %s" % endpoint)
        self.client=Client(endpoint)
        try:
            j=0
            for i,record in enumerate(self.client.listRecords(startdate)):
                self.process_record(record,init=True)
                self.lastcheckdate=record.responseDate()
                j=i
            self.logger.info("Finished adding  %d initial resources with checkdate: %s" % (j,self.lastcheckdate))
        except URLError, e:
            print "URLError: %s" % (e)
        except NoRecordsException as e:
            print "No new records found: %s" % e 
        self.check_for_updates()
    def _log_stats(self):
        """Log current source statistics"""
        stats = {
            'no_resources': self.resource_count,
            'no_events': self.no_events
        }
        self.logger.info("Source stats: %s" % stats)
    
    def process_record(self,record,init=False):
        """reads record, extract and returns record with information about (resource uri, timestamp, identifier)"""
        timestamp=Common.tofloat(record.header().datestamp())
        identifier=record.header().identifier()
        if(not record.header().isDeleted()):    #if resource new or updated
            basename=record.resource()
            if (identifier in self.oaimapping): # if update
                self.logger.debug("updating resource: identifier: %s basename: %s, timestamp %s" % (identifier, basename, timestamp))                    
                self._update_resource(basename,timestamp)
            else:                               # or create
                self.logger.debug("adding ressource: identifier: %s, basename %s, timestamp %s" % (identifier,
                                basename, timestamp))                    
                self._create_resource(basename,identifier,timestamp)
        elif(not init):
            self.logger.debug("deleting identifier: %s, timestamp %s" % (identifier, timestamp))                    
            self._delete_resource(identifier,timestamp)

        
    def check_for_updates(self):
        """Based on sleep_time and max_runs check on a given interval if the source has creations, updates, deletions"""
        for i in range (self.config['max_runs']):
            time.sleep(self.config['sleep_time'])
            self.logger.debug("Start with %d. run to check for updates at OAI with checkdate: %s" % 
                            (i,self.lastcheckdate))
            response_date=self.check(self.lastcheckdate)
            if response_date is not None:
                self.lastcheckdate=response_date
            time.sleep(self.config['sleep_time'])
        
    def check(self,checkdate):
        """check endpoint for new records
        filters records whose responseDate is lower as the last checkdate
        the filter is required since most endpoints work with finest granularity of days"""
        try:
            self.logger.debug("Requesting new records with date: %s" % checkdate)
            for i,record in enumerate(self.client.listRecords(checkdate)): # limit to specific date
                if self.lastcheckdate<=record.header().datestamp():
                    self.process_record(record)
                else:
                    self.logger.debug("Record %s has datestamp:%s but checkdate: %s is higher" % (record.header().identifier(), record.header().datestamp().strftime("%Y-%m-%dT%H:%M:%SZ"), self.lastcheckdate))
                checkdate=record.responseDate()
            return checkdate
        except NoRecordsException as e:
            print "No new records found: %s" % e            
             
        

    
    def __str__(self):
        """Prints out the source's resources"""
        return pprint.pformat(self._repository)