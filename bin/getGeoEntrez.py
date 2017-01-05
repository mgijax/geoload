#!/usr/local/bin/python
#
#  getGeoEntrez.py
###########################################################################
#
#  Purpose:
#
#      This script will generate the input file for the GEO load by
#      invoking the GEO query tool to get the EntrezGene IDs.
#
#  Usage:
#
#      getGeoEntrez.py
#
#  Env Vars:
#
#      GEOLOAD_INPUTFILE
#      GEOTOOL_DB
#      GEOTOOL_MAX_ROWS
#      GEOTOOL_URL
#
#  Inputs:  None
#
#  Outputs:
#
#      - Generated Input File (${GEOLOAD_INPUTFILE}) containing a list of
#        EntrezGene IDs
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#
#  Assumes:  Nothing
#
#  Notes:  None
#
###########################################################################
#
#  Modification History:
#
#  Date        SE   Change Description
#  ----------  ---  -------------------------------------------------------
#
#  07/07/2008  DBM  Initial development
#
###########################################################################

import sys
import os
import urllib
from xml.dom.minidom import parse

#
#  GLOBALS
#
outputFile = os.environ['GEOLOAD_INPUTFILE']
geoDB = os.environ['GEOTOOL_DB']
geoMaxRows = os.environ['GEOTOOL_MAX_ROWS']
geoURL = os.environ['GEOTOOL_URL']

#
# Establish the parameters for the GEO query tool.
#
params = urllib.urlencode(
   {'db': geoDB,
    'retmax': geoMaxRows,
    'term': 'gene_geoprofiles[filter] AND "Mus musculus"[organism]'})

params = urllib.urlencode(
   {'db': 'gene',
    'retmax': 100000,
    'term': 'gene_geoprofiles[filter] AND "Mus musculus"[organism]'})

#
# Open the output file.
#
try:
    fpOutputFile = open(outputFile, 'w')
except:
    print 'Cannot open output file: ' + outputFile
    sys.exit(1)

#
# Access the GEO query tools to get the EntrezGene IDs in XML format.
#
f = urllib.urlopen("%s%s" % (geoURL,params))
f = urllib.urlopen("http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?term=gene_geoprofiles%5Bfilter%5D+AND+%22Mus+musculus%22%5Borganism%5D&retmax=100000&db=gene")

print geoURL
print params
#
# Parse the XML document and close the query tool link.
#
doc = parse(f)
f.close()

#
# Get the list of EntrezGene IDs and write each one to the output file.
#
ids = doc.getElementsByTagName("Id")
for id in ids:
    fpOutputFile.write(id.firstChild.data + '\n')

#
# Close the output file.
#
fpOutputFile.close()

sys.exit(0)
