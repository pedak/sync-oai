import time
import urllib
import datetime

class Common(object):
    """containing common used functions"""
    
    @staticmethod
    def datestamp_to_date(datestamp):
        """converts datestamp in format YYYY-MM-DDTHH:MM:SSZ
        or format YYYY-MM-DD into datetime.datetime object"""
        if len(datestamp)>10:
            parts = datestamp.split('T')
            date, time = parts
            time = time[:-1] # remove final Z
            hours, minutes, seconds = time.split(':')
            year, month, day = date.split('-')
            return datetime.datetime(int(year), int(month), int(day), int(hours), int(minutes))
        else:
            date=datestamp
            year, month, day = date.split('-')
            return datetime.datetime(int(year), int(month), int(day)) 
        
    @staticmethod
    def tofloat(thedate):
        """convert datetime.datetime to float value"""
        return time.mktime(thedate.timetuple())
    
    @staticmethod
    def get_size(basename):
        """download header of basename and returns size in Bytes"""
        try:
            url_metadata=urllib.urlopen(basename).info()
            if len(url_metadata.getheaders("Content-Length"))>0:
                return url_metadata.getheaders("Content-Length")[0]
        except Exception, e:
            print "Exception at calculating size of %s %s" % (basename,e)
        return -1