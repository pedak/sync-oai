#!/usr/bin/env python
# encoding: utf-8
"""

"""

from urllib2 import urlopen, HTTPError
from urllib import urlencode
from xml.etree.ElementTree import ElementTree, Element, parse, tostring, ParseError
import re, time

OAI_NS="http://www.openarchives.org/OAI/2.0/"
DC_NS="http://purl.org/dc/elements/1.1/"



class Client(object):
    """OAI Client manages communication with an OAI-Endpoint"""
    
    def __init__(self,endpoint):
        self.endpoint=endpoint
    
    def listRecords(self,afrom=None):
        if afrom:
            params = urlencode({'verb': 'ListRecords', 'metadataPrefix': 'oai_dc', "from": afrom})
        else:
            params = urlencode({'verb': 'ListRecords', 'metadataPrefix': 'oai_dc'})
        while True:
                more = False
                try:
                    fh=urlopen(self.endpoint+"?"+params)
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
                try:
                    etree=parse(fh)
                    if (etree.getroot().tag == '{'+OAI_NS+"}OAI-PMH"): #check if it is an oai-pmh xml doc
                        rdate=etree.find('{'+OAI_NS+"}responseDate").text
                        listRecords=etree.find('{'+OAI_NS+"}ListRecords")
                        for xmlrecords in listRecords.findall('{'+OAI_NS+"}record"):
                                header_node=xmlrecords.find('{'+OAI_NS+"}header")
                                header=self.buildHeader(header_node)
                                metadata_node=xmlrecords.find('{'+OAI_NS+"}metadata")
                                if metadata_node:
                                    metadata=self.getIdentifier(metadata_node[0])
                                yield Record(header,metadata,rdate)
                        if(listRecords.find('{'+OAI_NS+"}resumptionToken") is not None):
                            rtoken=listRecords.find('{'+OAI_NS+"}resumptionToken").text
                            params = re.sub("&resumptionToken=.*","",params)    #delete previous resumptionToken
                            params += '&resumptionToken='+rtoken #add new resumptionToken
                        else:
                            break
                    
                    
                except ParseError, e:
                    print "ParseError %s" % e
                
                
                
    
    def buildHeader(self,header_node):
        identifier=None
        datestamp=None
        isdeleted=None
        for children in header_node:
            if children.tag=='{'+OAI_NS+'}identifier':
                identifier=children.text
            elif children.tag=='{'+OAI_NS+'}datestamp':
                datestamp=children.text
        if header_node.attrib=={'status': 'deleted'}:
            isdeleted=True
        return Header(identifier,datestamp,isdeleted)
    
    def getIdentifier(self,metadata_node):
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
    def __init__(self,header,metadata,response_date):
        self.header=header
        self.metadata=metadata
        self.response_date=response_date
    
    def __str__(self):
        return "Header: {%s}, Metadata: {%s}, Response-Date %s" % (self.header,self.metadata, self.response_date)

        
class Metadata(object):
    def __init__(self,relation,identifier):
        self._relation = relation
        self._identifier = identifier
    
    def identifier(self):
        return unicode(self._identifier)
    
    def relation(self):
        return self._relation
        
    def __str__(self):
        return "relation: %s, identifier: %s" % (self._relation,self._identifier) #.encode('ascii','ignore')
    
class Header(object):
    def __init__(self,identifier,datestamp,isdeleted):
        self._identifier = identifier
        self._datestamp = datestamp
        self._isdeleted = isdeleted
        
    def identifier(self):
        return self._identifier
    
    def datestamp(self):
        return self._datestamp

    def isDeleted(self):
        return self._isdeleted
    
    def __str__(self):
        return "identifier: %s, datestamp %s, isDeleted: %s" % (self._identifier, self._datestamp, self._isdeleted)

class Response(object):
    
    def __init__(self,rdate,records,rtoken):
          self.rdate=rdate
          self.records=records
          self.rtoken=rtoken
    
    def __str__(self):
        return "responseDate: %s, records %s, resumptionToken: %s" % (self.rdate, self.records, self.rtoken)
        

def main():
    client=Client("http://eprints.mminf.univie.ac.at/cgi/oai2")  
  #  client=Client("http://localhost/test.php")  
    #client=Client("http://export.arxiv.org/oai2")
#   client=Client("http://eprints.mminf.univie.ac.at/cgi/oai2?verb=ListRecords&metadataPrefix=oai_dc&from=2012-08-01")  
    #x=client.listRecords("2012-09-02")

    for i,y in enumerate(client.listRecords("2011-08-02")):
        print i,y
    # während loop werden neue noch aufgenommen.
    #for y in x.records:
    #    print y
    
          
        
if __name__ == '__main__':
    main()