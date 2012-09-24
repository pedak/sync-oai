#!/usr/bin/env python
# encoding: utf-8
"""
pyoai.py: The class handling OAI-PMH data access and resource information extraction

Created by Peter Kalchgruber on 2012-09-01.
"""

from urllib2 import urlopen, HTTPError, URLError
from urllib import urlencode
from xml.etree.ElementTree import  parse, ParseError, tostring
from time import sleep
import re
import urlparse
import datetime
import time
from dateutil import parser as dateutil_parser
from common import Common

OAI_NS="http://www.openarchives.org/OAI/2.0/"
DC_NS="http://purl.org/dc/elements/1.1/"

class Client(object):
    """OAI-PMH Client manages communication with an OAI-PMH endpoint"""
    
    def __init__(self,endpoint):
        self.endpoint=endpoint
        self.granularity=None
        m = urlparse.urlparse(endpoint)
        self.baseurl=m.netloc
        
    def get_date(self, datestring):
        """return datestamp of datetime.datetime object"""
        if self.granularity=="date":
            return datestring.strftime("%Y-%m-%d")
        else:
            return datestring.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    def setGranularity(self):
        """set granularity to source granularity"""
        try:
            params = urlencode({'verb': 'Identify'})
            fh=urlopen(self.endpoint+"?"+params)
            etree=parse(fh)
            if (etree.getroot().tag == '{'+OAI_NS+"}OAI-PMH"): #check if it is an oai-pmh xml doc
                identify_node=etree.find('{'+OAI_NS+"}Identify")
                granularity=identify_node.find('{'+OAI_NS+"}granularity").text
                if len(granularity)>10:
                    self.granularity="dateandtime"
                else:
                    self.granularity="date"
        except URLError, e:
            raise URLError("While opening URL: %s with parameters %s an error turned up %s" % (self.endpoint, params, e))
            
        except HTTPError, e:
            if e.code == 503:
                try:
                    retry = int(e.hdrs.get('Retry-After'))
                except TypeError:
                    retry = None
                if retry is None:
                    print "503-error without specification of time. Waiting 10 seconds"
                    time.sleep(10) #defaul sleep time
                else:
                    print "503-error waiting %s seconds" % retry
                    time.sleep(retry)
            else:
                raise IOError
                
        except AttributeError, e:
             print "Attribute Error (xml?) %s" % e
             
        except ParseError, e:
            print "ParseError %s" % e
    
    def listRecords(self,afrom=None,delay=0):
        """generator who list Records with informations about resources
        afrom can be datetime.datetime object or datestamp in format YYYY-MM-DDTHH:MM:SSZ
        """
        if afrom:
            if type(afrom)==datetime.datetime:  #if not datestamp convert to datestamp
                if self.granularity is None:
                    self.setGranularity()
                afrom=self.get_date(afrom)
                
            params = urlencode({'verb': 'ListRecords', 'metadataPrefix': 'oai_dc', "from": afrom})
        else:
            params = urlencode({'verb': 'ListRecords', 'metadataPrefix': 'oai_dc'})
            
        while True:
                try:
                    fh=urlopen(self.endpoint+"?"+params)
                    etree=parse(fh)
                    if (etree.getroot().tag == '{'+OAI_NS+"}OAI-PMH"): #check if it is an oai-pmh xml doc
                        rdate=dateutil_parser.parse(etree.find('{'+OAI_NS+"}responseDate").text)
                        for error in etree.findall('{'+OAI_NS+"}error"):
                            raise NoRecordsException, (error.attrib['code'],error.text)
                        listRecords=etree.find('{'+OAI_NS+"}ListRecords")
                        for xmlrecords in listRecords.findall('{'+OAI_NS+"}record"):
							header_node=xmlrecords.find('{'+OAI_NS+"}header")
							header=self.buildHeader(header_node)
							metadata_node=xmlrecords.find('{'+OAI_NS+"}metadata")
							resources=None
							if metadata_node is not None:
								resources=self.getIdentifiers(metadata_node[0])
								for resource in resources: # for each found resource in data record
									yield Record(header,resource,rdate)
							else: #e.g. in case of deletion
								yield Record(header,resource,rdate)
                        if(listRecords.find('{'+OAI_NS+"}resumptionToken") is not None):
                            rtoken=listRecords.find('{'+OAI_NS+"}resumptionToken").text
                            params = re.sub("&from=.*","",params)    #delete previous resumptionToken
                            params = re.sub("&resumptionToken=.*","",params)    #delete previous resumptionToken
                            params += '&resumptionToken='+rtoken #add new resumptionToken
                        else:
                            break
                        time.sleep(delay)

                except HTTPError, e:
                    if e.code == 503:
                        try:
                            retry = int(e.hdrs.get('Retry-After'))
                        except TypeError:
                            retry = None
                        if retry is None:
                            print "503-error without specification of time. Waiting 10 seconds"
                            time.sleep(10) #defaul sleep time
                        else:
                            print "503-error waiting the suggested %s seconds" % retry
                            for x in range(retry):
                                time.sleep(1)
                                print (x+1)
                    else:
                        raise IOError
                        
                except AttributeError, e:
                     print "Attribute Error (xml?) %s" % e
                     
                except ParseError, e:
                    print "ParseError %s" % e
                
                except URLError, e:
                    raise URLError("While opening URL: %s with parameters %s an error turned up %s" % (self.endpoint, params, e))
                
    def buildHeader(self,header_node):
        """extract header information of header_node into Header object"""
        identifier=None
        datestamp=None
        isdeleted=None
        for children in header_node:
            if children.tag=='{'+OAI_NS+'}identifier':
                identifier=children.text
            elif children.tag=='{'+OAI_NS+'}datestamp':
                if re.match(r"\d\d\d\d\-\d\d\-\d\d$",children.text):
                    children.text+="T00:00:00Z"
                if re.match(r"\d\d\d\d\-\d\d\-\d\dT\d\d:\d\d(:\d\d)?(Z|[+-]\d\d:\d\d)$",children.text):
                    datestamp=dateutil_parser.parse(children.text)
                else:
                    raise ValueError("Bad datestamp format (%s)" % children.text)
        if header_node.attrib=={'status': 'deleted'}:
            isdeleted=True
        return Header(identifier,datestamp,isdeleted)
    
    def getIdentifiers(self,metadata_node):
        """extract resource information of metadata_node"""
        identifiers=[]
        for children in metadata_node.findall('{'+DC_NS+'}identifier'):
            identifiers.append(children.text)
        for children in metadata_node.findall('{'+DC_NS+'}relation'):
            identifiers.append(children.text)
        resources={}
        for identifier in identifiers:
            if re.match("http.*"+self.baseurl+"[-A-Za-z0-9+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]",identifier) is not None:
                resource=re.sub("/$","",identifier)
                return {resource: resource} #should be extended #debug
        
class Record(object):
    """record about resource"""
    
    def __init__(self,header,resource,response_date):
        self._header=header
        self._resource=resource
        self._response_date=response_date
        
    def header(self):
        """header data about resource"""
        return self._header
    
    def resource(self):
        """resource uri"""
        return self._resource
    
    def responseDate(self):
        """response date of OAI-PMH endpoint of the response containing this record"""
        return self._response_date
    
    def id(self):
        """shortcut for identifier of header of record"""
        return self.header().identifier()
    
    def __eq__(self,other):
        return self.header() == other.header()
    
    def __str__(self):
        """Prints out Header, response date and resource uri """
        return "Header: {%s}, Resource: {%s}, Response-Date %s" % (self._header,self._resource, self._response_date)

        
class Header(object):
    """header informations"""
    
    def __init__(self,identifier,datestamp,isdeleted=False):
        self._identifier = identifier
        self._datestamp = datestamp
        self._isdeleted = isdeleted
        
    def identifier(self):
        """Identifier of record e.g. oai:eprints.org:3218"""
        return self._identifier
    
    def datestamp(self):
        """datestamp in format datetime.datetime"""
        return self._datestamp
    
    
    def isDeleted(self):
        """status of record, set True if it is deleted"""
        return self._isdeleted
    
    def __eq__(self,other):
        return self.identifier()==other.identifier()
    
    def __str__(self):
        """Prints out the Header attributes"""
        return "identifier: %s, datestamp %s, isDeleted: %s" % (self._identifier, self._datestamp, self._isdeleted)
        
        



class NoRecordsException(Exception):
    pass

         