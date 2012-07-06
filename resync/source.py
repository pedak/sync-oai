#!/usr/bin/env python
# encoding: utf-8
"""
source.py: A source holds a set of resources and changes over time.

Resources are internally stored by their basename (e.g., 1) for memory
efficiency reasons.

Created by Bernhard Haslhofer on 2012-04-24.
Copyright 2012, ResourceSync.org. All rights reserved.
"""

import os.path
import random
import pprint
import logging
import time

from apscheduler.scheduler import Scheduler

from observer import Observable
from change import ChangeEvent
from resource import Resource
from digest import compute_md5_for_string
from inventory import Inventory
from resync.sitemap import Sitemap
from resync.sitemap import Mapper

class Source(Observable):
    """A source contains a list of resources and changes over time"""
    
    RESOURCE_PATH = "/resources"
    STATIC_FILE_PATH = os.path.join(os.path.dirname(__file__), "static")
    
    def __init__(self, config, hostname, port):
        """Initalize the source"""
        super(Source, self).__init__()
        self.config = config
        self.hostname = hostname
        self.port = port
        self.max_res_id = 1
        self._repository = {} # {basename, {timestamp, size}}
        self.changememory = None # The change memory implementation
        self._bootstrap()
        if self.config['inventory']['type'] == 'static':
            interval = self.config['inventory']['interval']
            logging.basicConfig()
            sched = Scheduler()
            sched.start()
            sched.add_interval_job(self.write_static_inventory,
                                    seconds=interval)
        
    # Public Methods

    def write_static_inventory(self):
        """Writes the inventory to the filesystem"""
        basename = Source.STATIC_FILE_PATH + "/sitemap.xml"
        then = time.time()
        s=Sitemap()
        s.max_sitemap_entries=500
        s.mapper=Mapper([self.base_uri + "", Source.STATIC_FILE_PATH])
        s.write(self.inventory, basename)
        now = time.time()
        print "Wrote static sitemap in %s seconds" % str(now-then)
    
    def add_changememory(self, changememory):
        """Adds a changememory implementation"""
        self.changememory = changememory
        
    @property
    def has_changememory(self):
        """Returns True if a source maintains a change memory"""
        return bool(self.changememory is not None)
    
    @property
    def resource_count(self):
        """The number of resources in the source's repository"""
        return len(self._repository)
    
    @property
    def inventory_path(self):
        """The inventory path (from the config file)"""
        return self.config['inventory']['uri_path']
    
    @property
    def inventory_uri(self):
        """The inventory URI (e.g., http://localhost:8080/sitemamp.xml)"""
        return self.base_uri + "/" + self.inventory_path
    
    @property
    def inventory(self):
        """Returns a snapshot of all resources in the repository"""
        inventory = Inventory()
        for resource in self.resources:
            if resource is not None: inventory.add(resource)
        
        if self.has_changememory:
            next_changeset = self.changememory.next_changeset_uri
            inventory.capabilities[next_changeset] = {"type": "changeset"}
        
        return inventory
    
    @property
    def base_uri(self):
        """Returns the base URI of the source (e.g., http://localhost:8888)"""
        return "http://" + self.hostname + ":" + str(self.port)
    
    @property
    def resources(self):
        """Iterates over resources and yields resource objects"""
        repository = self._repository
        for basename in repository.keys():
            resource = self.resource(basename)
            if resource is None:
                print "Cannot create resource %s " % basename + \
                      "because source object has been deleted." 
            yield resource
    
    def resource(self, basename):
        """Creates and returns a resource object from internal resource
        repository. Repositoy values are copied into the object."""
        if not self._repository.has_key(basename): return None
        uri = self.base_uri + Source.RESOURCE_PATH + "/" + basename
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
        print "*** Starting change simulation with frequency %s and event " \
                "types %s ***" \
                 % (str(round(self.config['change_frequency'], 2)), 
                    self.config['event_types'])
        no_events = 0
        sleep_time = round(float(1) / self.config['change_frequency'], 2)
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
                    print "The repository is empty"
                    no_events = no_events + 1                    
                    continue
                if event_type == "update":
                    self._update_resource(basename)
                elif event_type == "delete":
                    self._delete_resource(basename)

            else:
                print "Event type %s is not supported" % event_type
            no_events = no_events + 1

        print "*** Finished change simulation ***"
    
    # Private Methods
    
    def _create_resource(self, basename = None, notify_observers = True):
        """Create a new resource, add it to the source, notify observers."""
        if basename == None:
            basename = str(self.max_res_id)
            self.max_res_id += 1
        timestamp = time.time()
        size = random.randint(0, self.config['average_payload'])
        self._repository[basename] = {'timestamp': timestamp, 'size': size}
        if notify_observers:
            event = ChangeEvent("CREATE", self.resource(basename))
            self.notify_observers(event)
        
    def _update_resource(self, basename):
        """Update a resource, notify observers."""
        self._delete_resource(basename, notify_observers = False)
        self._create_resource(basename, notify_observers = False)
        event = ChangeEvent("UPDATE", self.resource(basename))
        self.notify_observers(event)

    def _delete_resource(self, basename, notify_observers = True):
        """Delete a given resource, notify observers."""
        res = self.resource(basename)
        del self._repository[basename]
        res.timestamp = time.time()
        if notify_observers:
            event = ChangeEvent("DELETE", res)
            self.notify_observers(event)
    
    def _bootstrap(self):
        """Bootstrap the source with a set of resources"""
        print "*** Bootstrapping source with %d resources and an average " \
                "resource payload of %d bytes ***" \
                 % (self.config['number_of_resources'],
                    self.config['average_payload'])

        for i in range(self.config['number_of_resources']):
            self._create_resource(notify_observers = False)

    def __str__(self):
        """Prints out the source's resources"""
        return pprint.pformat(self._repository)
