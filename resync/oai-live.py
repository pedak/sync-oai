from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
registry = MetadataRegistry()
registry.registerReader('oai_dc', oai_dc_reader)
#client = Client("http://eprints.erpanet.org/perl/oai2", registry)
client = Client("http://eprints.mminf.univie.ac.at/cgi/oai2", registry)
import date
date=datetime.datetime.now()
#date=date.strftime("%Y-%m-%d %H:%S:Z")
client.listRecords(metadataPrefix='oai_dc', from_=date)