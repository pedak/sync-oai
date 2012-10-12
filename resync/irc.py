#!/usr/bin/env python
# encoding: utf-8
"""
irc-adapter: The ResourceSync command line tool for wikimedia synchronization.

Created by Peter Kalchgruber on 2012-09-01.
"""

import optparse
import yaml
import logging
import sys #List of pre-made libararies to import
import socket
import string
import re
from time import sleep

from resync.source import Source
from resync.http import HTTPInterface
from resync.util import UTCFormatter

DEFAULT_CONFIG_FILE = 'config/default.yaml'
SOURCE_LOG_FILE = 'resync-source.log'


class IRCClient():
    def __init__(self,host,channel,nick,ident,realname):
        self.host=host
        self.channel=channel
        self.connected=False
        self.nick=nick
        self.ident=ident
        self.realname=realname
        
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
            
            
    def connect(self):
        port=6667
        self.s=socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP ) 
        self.s.connect((self.host, port)) 
        self.connected=True
        self.s.sendall("NICK %s\r\n" % self.nick) 
        self.s.sendall("USER %s %s as :%s\r\n" % (self.ident, self.host, self.realname)) 
        self.s.send("JOIN :#%s\r\n" % self.channel) 
        return self.s.makefile()
      
    def send(self,message):
        self.s.send(message)
        
    def sendall(self, message):
        self.s.sendall(message)
        
    def disconnect(self):
        if self.connected:
            self.s.send("/quit")
                
def main():    
    try:
        irc=IRC()
        irc.connect()
    except KeyboardInterrupt:
        print "\nStopping adapter and exiting gracefully..."
    finally:
        irc.disconnect()

if __name__ == '__main__':
    main()