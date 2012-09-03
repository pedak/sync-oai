#!/usr/bin/env python
# encoding: utf-8
"""

"""

from urllib import URLopener
from urllib import URLopener
from xml.etree.ElementTree import ElementTree, Element, parse, tostring

OAI_NS="http://www.openarchives.org/OAI/2.0/"
DC_NS="http://purl.org/dc/elements/1.1/"


class Test(object):
    def __init__(self):
        x=None
    
    def do(self):
        x=URLopener().open("http://kalchgruber.com/test.php?id=0")
        y=x.read().rsplit(",")
        while 1:
            for z in y:
                yield z
            id=y[len(y)-1]
            x=URLopener().open("http://kalchgruber.com/test.php?id="+id)
            y=x.read().rsplit(",")

        

def main():
    t=Test()
    print t.do().next()
    
    
                
if __name__ == '__main__':
    main()