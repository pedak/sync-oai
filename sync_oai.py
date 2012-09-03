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


import tornado.httpserver
import tornado.ioloop
import tornado.web
from tornado.options import define, options
import os
import threading

from resync.source import Source
from resync.http import HTTPInterface
from resync.oai import OAI

DEFAULT_OAI_ENDPOINT = 'http://eprints.ucm.es/cgi/oai2'
DEFAULT_OAI_ENDPOINT = "http://eprints.erpanet.org/perl/oai2"


define("port", default=5555, help="Rund on specific port", type=int)

class MainHandler(tornado.web.RequestHandler):

  def post(self):
      print os.path.join(os.path.dirname(__file__))
      self.write(os.path.join(os.path.dirname(__file__)))

settings = {
  "static_path": os.path.join(os.path.dirname(__file__), "tmp"),
}

application = tornado.web.Application(
                handlers = [(r"/(sitemap\d*\.xml)",tornado.web.StaticFileHandler,dict(path = 'resync/sitemap/'))], 
                debug = True,
                **settings)




def main():
    try:
        threading.Thread(target=startTornado).start()
        oai()
        import time
        time.sleep(9999999)
    except KeyboardInterrupt:
        print "Exiting gracefully..."
    finally:
        stopTornado()

    
def oai():
    # Define oai options
    parser = argparse.ArgumentParser(
                description = "OAI Endpoint Adapter")
    parser.add_argument('--endpoint', '-e', 
        default=DEFAULT_OAI_ENDPOINT,
        help="the url configuration file")

    # Parse command line arguments
    args = parser.parse_args()

    # Load the logging configuration file and set up logging
    endpoint = args.endpoint
    oai=OAI(endpoint)
    oai.create_sitemaps()

def startTornado():
    tornado.options.parse_command_line()
    application.listen(5555)
    tornado.ioloop.IOLoop.instance().start()

def stopTornado():
    tornado.ioloop.IOLoop.instance().stop()


    
    
    
if __name__ == '__main__':
    main()