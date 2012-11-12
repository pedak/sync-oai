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
import random

import time

class IRCClient():
    def __init__(self,host,channel,nick,ident,realname):
        self.host=host
        self.channel=channel
        self.connected=False
        self.nick=nick
        self.ident=ident
        self.realname=realname
            
            
    def connect(self):
        port=6667
        self.s=socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP ) 
        self.s.connect((self.host, port)) 
        self.connected=True
    
    def register(self,randomnick=False):
        nick=self.nick
        if randomnick:
            nick = "%s%s" % (nick,str(random.randint(10, 99)))
        self.s.sendall("NICK %s\r\n" % nick) 
        self.s.sendall("USER %s %s as :%s\r\n" % (self.ident, self.host, self.realname)) 
        self.s.send("JOIN :#%s\r\n" % self.channel) 
        return self.s.makefile()
    
    def send(self,message):
        self.s.send(message)
        
    def sendall(self, message):
        print message
        self.s.sendall(message)

        
    def disconnect(self):
        if self.connected:
            self.s.send("/quit")
                