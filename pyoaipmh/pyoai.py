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
import datetime

from common import Common

OAI_NS="http://www.openarchives.org/OAI/2.0/"
DC_NS="http://purl.org/dc/elements/1.1/"

class Client(object):
    """OAI Client manages communication with an OAI-Endpoint"""
    
    def __init__(self,endpoint):
        self.endpoint=endpoint
    
    def get_datestamp(self, datestring):
        """return datestamp of datetime.datetime object"""
        return datestring.strftime("%Y-%m-%dT%H:%M:%SZ")
        
    def listRecords(self,afrom=None):
        """generator who list Records with informations about resources
        afrom can be datetime.datetime object or datestamp in format YYYY-MM-DDTHH:MM:SSZ
        """
        if afrom:
            if type(afrom)==datetime.datetime:  #if not datestamp convert to datestamp
                afrom=self.get_datestamp(afrom)
            params = urlencode({'verb': 'ListRecords', 'metadataPrefix': 'oai_dc', "from": afrom})
        else:
            params = urlencode({'verb': 'ListRecords', 'metadataPrefix': 'oai_dc'})
            
        while True:
                try:
                    fh=urlopen(self.endpoint+"?"+params)
                    etree=parse(fh)
                    if (etree.getroot().tag == '{'+OAI_NS+"}OAI-PMH"): #check if it is an oai-pmh xml doc
                        rdate=etree.find('{'+OAI_NS+"}responseDate").text
                        for error in etree.findall('{'+OAI_NS+"}error"):
                            raise NoRecordsException, (error.attrib['code'],error.text)
                        listRecords=etree.find('{'+OAI_NS+"}ListRecords")
                        for xmlrecords in listRecords.findall('{'+OAI_NS+"}record"):
                                header_node=xmlrecords.find('{'+OAI_NS+"}header")
                                header=self.buildHeader(header_node)
                                metadata_node=xmlrecords.find('{'+OAI_NS+"}metadata")
                                resource=None
                                if metadata_node is not None:
                                    resource=self.getIdentifier(metadata_node[0])
                                yield Record(header,resource,rdate)
                        if(listRecords.find('{'+OAI_NS+"}resumptionToken") is not None):
                            rtoken=listRecords.find('{'+OAI_NS+"}resumptionToken").text
                            params = re.sub("&resumptionToken=.*","",params)    #delete previous resumptionToken
                            params += '&resumptionToken='+rtoken #add new resumptionToken
                        else:
                            break
            
                except URLError, e:
                    raise URLError("While opening URL: %s with parameters %s an error turned up %s" % (self.endpoint, params, e))
                    
                except HTTPError, e:
                    if e.code == 503:
                        try:
                            retry = int(e.hdrs.get('Retry-After'))
                        except TypeError:
                            retry = None
                        if retry is None:
                            print "503-error waiting 10 seconds"
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
                
    def buildHeader(self,header_node):
        """extract header information of header_node into Header object"""
        identifier=None
        datestamp=None
        isdeleted=None
        for children in header_node:
            if children.tag=='{'+OAI_NS+'}identifier':
                identifier=children.text
            elif children.tag=='{'+OAI_NS+'}datestamp':
                datestamp=Common.datestamp_to_date(children.text)
        if header_node.attrib=={'status': 'deleted'}:
            isdeleted=True
        return Header(identifier,datestamp,isdeleted)
    
    def getIdentifier(self,metadata_node):
        """extract resource information of metadata_node"""
        identifiers=[]
        for children in metadata_node.findall('{'+DC_NS+'}identifier'):
            identifiers.append(children.text)
        for children in metadata_node.findall('{'+DC_NS+'}relation'):
            identifiers.append(children.text)
        for identifier in identifiers:
            url=re.search("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]",identifier)
            if(url):
                return url.group()
        return None
    

     
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
    
    def __str__(self):
        """Prints out the Header attributes"""
        return "identifier: %s, datestamp %s, isDeleted: %s" % (self._identifier, self._datestamp, self._isdeleted)



class NoRecordsException(Exception):
    pass

def main():
    client=Client("http://eprints.mminf.univie.ac.at/cgi/oai2")  
  #  client=Client("http://localhost/test.php")  
    #client=Client("http://export.arxiv.org/oai2")
#   client=Client("http://eprints.mminf.univie.ac.at/cgi/oai2?verb=ListRecords&metadataPrefix=oai_dc&from=2012-08-01")  
    #x=client.listRecords("2012-09-02")

    for i,y in enumerate(client.listRecords("2013-08-02T01:01:01Z")):
        print i,y
          
        
if __name__ == '__main__':
    main()