#!/usr/bin/env python
# encoding: utf-8
"""
dbpedia.py: Manages simulation of changes at DBpedia via dbpedia-live 

Created by Peter Kalchgruber on 2012-07-16.
"""

import re
import os
import random
import pprint
import logging
import time
import gzip
import StringIO
import urllib
import datetime
import rdflib
from rdflib.compare import to_isomorphic, graph_diff


from resync.resource import Resource
from resync.inventory import Inventory, InventoryDupeError
from resync.sitemap import Sitemap, SitemapIndexError, Mapper
from resync.resource_change import ResourceChange
from resync.changememory import DynamicChangeSet

from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
import shutil
import re 




# http://eprints.erpanet.org/perl/oai2
class OAI():
    

    
    def __init__(self, endpoint):
        params = urllib.urlencode({'verb': 'ListRecords', 'metadataPrefix': 'oai_dc'})
        registry = MetadataRegistry()
        registry.registerReader('oai_dc', oai_dc_reader)
        self.client = Client(endpoint, registry)
        client = Client("http://eprints.erpanet.org/perl/oai2", registry) # DEBUG
        #self.client = Client("http://eprints.mminf.univie.ac.at/cgi/oai2", registry)
        
        resources=[]
        
        self.client.updateGranularity()
        sitemap_dir="sitemap/"
        """Create sitemap directory if it doesn't exist; removes existing one"""
        if os.path.exists(sitemap_dir):
            shutil.rmtree(sitemap_dir)
            os.makedirs(sitemap_dir)
        else:
            os.makedirs(sitemap_dir)
    
        config = {'uri_path': 'changeset', 'class': 'DynamicChangeSet', 'max_changes': 1000}
        changememory = DynamicChangeSet(self,config)
        changememory.bootstrap();
    
    def get_identifier(self,record):
        for identifier in record[1].getField("identifier"):
            if (re.match("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]",identifier)):
                print "id found:%s "% id
                return str(identifier)

        for relation in record[1].getField("relation"):
            if (re.match("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]",relation)):
                print "relation found:%s "% id
                return str(relation)

        for identifier in record[1].getField("identifier"):
            if (re.search("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]",identifier)):
                print "id extracted of identifier"
                return re.search("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]",identifier).group()
        
        return ""        
                
    def get_datestamp(self, record):
        return record[0].datestamp().strftime("%Y-%m-%dT%H:%M:%S+00:00")
    
    def create_sitemaps(self):
        SITEMAP_FILE_PATH = os.path.join(os.path.dirname(__file__), "sitemap")
        inv=Inventory()
        for i,record in enumerate(self.client.listRecords(metadataPrefix='oai_dc')):
            #search for identifier in fields identifier and relation
            if(record[1]):# and re.match("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]",record[1].getField("identifier")[0])):
                id=self.get_identifier(record)        
                datestamp=self.get_datestamp(record)
                print "i: %s identifier: %s, datestamp %s" % (i, id, datestamp)
                if len(id)>0 and datestamp:
                    r=Resource(uri=id, lastmod=datestamp)
                    print "adding resource %s" % r
                    try:
                        inv.add(r)
                    except InventoryDupeError, ex:
                        print "Inventory Error: %s" % ex
                
        s=Sitemap() 
        basename = SITEMAP_FILE_PATH + "/sitemap.xml"
        s.max_sitemap_entries=1000
        
        s.mapper=Mapper(["http://localhost:5555/", SITEMAP_FILE_PATH])
        s.write(inv, basename)
    
    def create_changesets(self,date):
        for i,record in enumerate(self.client.listRecords(metadataPrefix='oai_dc', from_=date)): # limit to specific date
            id=self.get_identifier(record)        
            datestamp=self.get_datestamp(record)
            print "i: %s identifier: %s, datestamp %s" % (i, id, datestamp)
            if(record[0].isDelete()):
                    change = ResourceChange(
                        resource = self.resource(basename),
                        changetype = "CREATE")
                    self.notify_observers(change)
            else:
                change = ResourceChange(
                            resource = self.resource(basename),
                            changetype = "CREATE")
                self.notify_observers(change)
    def register_observer(var,self):
        print "do nothing: " % var
            

def main():
    fromdate=datetime.datetime(2009,1,1)
    oai=OAI("http://eprints.erpanet.org/perl/oai2")
    oai.create_changesets(fromdate)


if __name__ == '__main__':
    main()    