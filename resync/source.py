#!/usr/bin/env python
# encoding: utf-8
"""
source.py: A source holds a set of resources and changes over time.

Resources are internally stored by their basename (e.g., 1) for memory
efficiency reasons.

Created by Bernhard Haslhofer on 2012-04-24.
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
from resync.digest import compute_md5_for_string
from resync.inventory import Inventory
from resync.sitemap import Sitemap, Mapper

##oai
# from pyoai.client import Client
# from pyoai.metadata import MetadataRegistry, oai_dc_reader
# from pyoai.error import NoRecordsMatchError
from pyoaipmh.pyoai import Client, Header, Record

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
        self.logger.debug("Start inventory generation")
        capabilities = {}
        if self.source.has_changememory:
            next_changeset = self.source.changememory.next_changeset_uri()
            capabilities[next_changeset] = {"type": "changeset"}
        inventory = Inventory(resources=self.source.resources,
                              capabilities=capabilities)
        self.logger.debug("Finished inventory generation")
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
        # Log Sitemap create start event
        sitemap_size = self.compute_sitemap_size(Source.STATIC_FILE_PATH)
        self.logger.info("Finished writing Sitemap inventory")
        sm_write_end = ResourceChange(
                resource = ResourceChange(self.uri, 
                                size=sitemap_size,
                                timestamp=time.time()),
                                changetype = "SITEMAP UPDATE END")
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
        self.oaimapping = {} #oai
    
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
        self.logger.info("Bootstrapping source with %d resources " \
                "and an average resource payload of %d bytes" \
                 % (self.config['number_of_resources'],
                    self.config['average_payload']))
        
        if self.has_changememory: self.changememory.bootstrap()
        if self.has_inventory_builder: self.inventory_builder.bootstrap()

    
    def bootstrap_original(self):
        """Bootstrap the source with a set of resources"""
        self.logger.info("Bootstrapping source with %d resources " \
                "and an average resource payload of %d bytes" \
                 % (self.config['number_of_resources'],
                    self.config['average_payload']))

        for i in range(self.config['number_of_resources']):
            self._create_resource(notify_observers = False)
            
        if self.has_changememory: self.changememory.bootstrap()
        if self.has_inventory_builder: self.inventory_builder.bootstrap()
    
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
        repository = self._repository
        for basename in repository.keys():
            resource = self.resource(basename)
            if resource is None:
                self.logger.error("Cannot create resource %s " % basename + \
                      "because source object has been deleted.")
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
        md5 = compute_md5_for_string(self.resource_payload(basename, size))
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
        self.logger.info("Starting simulation with change delay %s " \
              "and event types %s ***" % (str(self.config['change_delay']),
                                          self.config['event_types']))
        no_events = 0
        sleep_time = self.config['change_delay']
        while no_events != self.config['max_events']:
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
                    no_events = no_events + 1                    
                    continue
                if event_type == "update":
                    self._update_resource(basename)
                elif event_type == "delete":
                    self._delete_resource(basename)

            else:
                self.logger.error("Event type %s is not supported" 
                                                                % event_type)
            no_events = no_events + 1

        print "*** Finished change simulation ***"
    
    # Private Methods
    
    def _create_resource_original(self, basename = None, notify_observers = True):
        """Create a new resource, add it to the source, notify observers."""
        if basename == None:
            basename = str(self.max_res_id)
            self.max_res_id += 1
        timestamp = time.time()
        size = random.randint(0, self.config['average_payload'])
        self._repository[basename] = {'timestamp': timestamp, 'size': size}
        if notify_observers:
            change = ResourceChange(
                        resource = self.resource(basename),
                        changetype = "CREATE")
            self.notify_observers(change)
    
    def _create_resource(self, basename = None, timestamp=time.time(), notify_observers = True):
        """Create a new resource, add it to the source, notify observers."""
        self._repository[basename] = {'timestamp': timestamp}
        if notify_observers:
            change = ResourceChange(
                        resource = self.resource(basename),
                        changetype = "CREATE")
            self.notify_observers(change)
        
    def _update_resource(self, basename):
        """Update a resource, notify observers."""
        self._delete_resource(basename, notify_observers = False)
        self._create_resource(basename, notify_observers = False)
        change = ResourceChange(
                    resource = self.resource(basename),
                    changetype = "UPDATE")
        self.notify_observers(change)

    def _delete_resource(self, basename, notify_observers = True):
        """Delete a given resource, notify observers."""
        res = self.resource(basename)
        del self._repository[basename]
        res.timestamp = time.time()
        if notify_observers:
            change = ResourceChange(
                        resource = res,
                        changetype = "DELETE")
            self.notify_observers(change)
    
    def bootstrap_oai(self,endpoint):
        time.sleep(2000)
        # registry = MetadataRegistry()
        # registry.registerReader('oai_dc', oai_dc_reader)
        # self.client = Client(endpoint, registry)
        # client = Client("http://eprints.erpanet.org/perl/oai2", registry) # DEBUG
        # #self.client = Client("http://eprints.mminf.univie.ac.at/cgi/oai2", registry)
        # self.client = Client("http://eprints.mminf.univie.ac.at/cgi/oai2", registry)
        # resources=[]
        # import datetime
        # self.client.updateGranularity()
        # date=datetime.datetime(2012,01,01)
        # #for i,record in enumerate(self.client.listRecords(metadataPrefix='oai_dc')):
        # for i,record in enumerate(self.client.listRecords(metadataPrefix='oai_dc', from_=date)): # limit to specific date
        #     #search for identifier in fields identifier and relation
        #     self.check_record(record,init=True)
        #                 
        # checkdate=datetime.datetime.now() #debug local time != server time    
        # while(True):
        #     time.sleep(10)
        #     self.check(checkdate)
        #     checkdate=datetime.datetime.now() #debug local time != server time
        #     print checkdate.strftime("%Y-%m-%dT%H:%M:%S")
        #                     
        # time.sleep(99999)
        
    def check(self,date):
        try:
            for i,record in enumerate(self.client.listRecords(metadataPrefix='oai_dc', from_=date)): # limit to specific date
                self.check_record(record)
        except NoRecordsMatchError as e:
            print "No new records found: %s" % e
            
    def check_record(self,record,init=False):
        if(record[1]):# and re.match("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]",record[1].getField("identifier")[0])):
                basename=self.get_identifier(record)        
                timestamp=self.get_timestamp(record)
                print "identifier: %s, timestamp %s" % (basename, timestamp)
                if len(basename)>0 and timestamp:
                    if(not record[0].isDeleted()):
                        self._repository[basename] = {'timestamp': timestamp, 'size': 0}
                        # write local mapping file
                        self.oaimapping[record[0].identifier()]=basename;
                        change = ResourceChange(
                            resource = self.resource(basename),
                            changetype = "CREATE")
                        self.notify_observers(change)
                    elif(not init):
                        res = self.resource(basename)
                        del self._repository[basename]
                        res.timestamp = time.time()
                        if notify_observers:
                            change = ResourceChange(
                                        resource = res,
                                        changetype = "DELETE")
                            self.notify_observers(change)
        
    def get_identifier(self,record):
        for identifier in record[1].getField("identifier"):
            if (re.match("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]",identifier)):
                print "basename found:%s "% id
                return str(identifier)

        for relation in record[1].getField("relation"):
            if (re.match("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]",relation)):
                print "relation(=basename) found:%s "% id
                return str(relation)

        for identifier in record[1].getField("identifier"):
            if (re.search("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]",identifier)):
                print "basename extracted of identifier"
                return re.search("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]",identifier).group()
        
        return ""        
                
    def get_timestamp(self, record):
        return time.mktime(record[0].datestamp().timetuple())
        
    
    def __str__(self):
        """Prints out the source's resources"""
        return pprint.pformat(self._repository)