#!/usr/local/bin/python

#
#  geoload.py
###########################################################################
#
#  Purpose:
#
#      This script will create a bcp file for the ACC_Accession table
#      that contains associations between EntrezGene IDs and markers.
#
#  Usage:
#
#      geoload.py
#
#  Env Vars:
#
#      MGD_DBUSER
#      MGD_DBPASSWORDFILE
#      GEOLOAD_INPUTFILE
#      GEOLOAD_RPTFILE
#      GEOLOAD_ACC_BCPFILE
#      GEO_TEMP_TABLE
#      GEO_LOGICAL_DB
#      EG_LOGICAL_DB
#      MARKER_MGITYPE
#      GEO_CREATED_BY
#
#  Inputs:
#
#      - Input file (${GEOLOAD_INPUTFILE}) containing a list of EntrezGene IDs
#
#  Outputs:
#
#      - BCP file (${GEOLOAD_ACC_BCPFILE}) for the ACC_Accession table
#        containing the associations between EntrezGene IDs and markers
#
#      - Discrepancy report (${GEOLOAD_RPTFILE})
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
#  07/11/2008  DBM  Initial development
#
###########################################################################

import sys
import os
import string
import re
import accessionlib
import loadlib
import mgi_utils
import db

#
#  CONSTANTS
#
TAB = '\t'
NL = '\n'

PRIVATE = '1'
PREFERRED = '1'

#
#  GLOBALS
#
user = os.environ['MGD_DBUSER']
passwordFile = os.environ['MGD_DBPASSWORDFILE']
inputFile = os.environ['GEOLOAD_INPUTFILE']
rptFile = os.environ['GEOLOAD_RPTFILE']
accBCPFile = os.environ['GEOLOAD_ACC_BCPFILE']

tempTable = os.environ['GEO_TEMP_TABLE']
geoLogicalDB = os.environ['GEO_LOGICAL_DB']
egLogicalDB = os.environ['EG_LOGICAL_DB']
markerMGIType = os.environ['MARKER_MGITYPE']
createdBy = os.environ['GEO_CREATED_BY']

loadDate = loadlib.loaddate
timestamp = mgi_utils.date()


#
# Purpose: Perform initialization steps.
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def init ():
    global accKey, geoLogicalDBKey, egLogicalDBKey
    global markerMGITypeKey, createdByKey

    db.set_sqlUser(user)
    db.set_sqlPasswordFromFile(passwordFile)
    db.useOneConnection(1)

    #
    # Get the keys from the database.
    #
    cmds = []
    cmds.append('select max(_Accession_key) + 1 "_Accession_key" from ACC_Accession')

    cmds.append('select _LogicalDB_key from ACC_LogicalDB ' + \
                'where name = "%s"' % (geoLogicalDB))

    cmds.append('select _LogicalDB_key from ACC_LogicalDB ' + \
                'where name = "%s"' % (egLogicalDB))

    cmds.append('select _MGIType_key from ACC_MGIType ' + \
                'where name = "%s"' % (markerMGIType))

    cmds.append('select _User_key from MGI_User ' + \
                'where name = "%s"' % (createdBy))

    results = db.sql(cmds,'auto')

    #
    # If any of the keys cannot be found, stop the load.
    #
    if len(results[0]) == 1:
        accKey = results[0][0]['_Accession_key']
    else:
        print 'Cannot determine the next Accession key'
        sys.exit(1)

    if len(results[1]) == 1:
        geoLogicalDBKey = results[1][0]['_LogicalDB_key']
    else:
        print 'Cannot determine the Logical DB key for "' + geoLogicalDB + '"'
        sys.exit(1)

    if len(results[2]) == 1:
        egLogicalDBKey = results[2][0]['_LogicalDB_key']
    else:
        print 'Cannot determine the Logical DB key for "' + egLogicalDB + '"'
        sys.exit(1)

    if len(results[3]) == 1:
        markerMGITypeKey = results[3][0]['_MGIType_key']
    else:
        print 'Cannot determine the MGI Type key for "' + markerMGIType + '"'
        sys.exit(1)

    if len(results[4]) == 1:
        createdByKey = results[4][0]['_User_key']
    else:
        print 'Cannot determine the User key for "' + createdBy + '"'
        sys.exit(1)

    return


#
# Purpose: Open the files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def openFiles ():
    global fpInputFile, fpRptFile, fpAccBCPFile

    #
    # Open the input file.
    #
    try:
        fpInputFile = open(inputFile, 'r')
    except:
        print 'Cannot open input file: ' + inputFile
        sys.exit(1)

    #
    # Open the report file.
    #
    try:
        fpRptFile = open(rptFile, 'w')
    except:
        print 'Cannot open report file: ' + rptFile
        sys.exit(1)

    #
    # Open the output file.
    #
    try:
        fpAccBCPFile = open(accBCPFile, 'w')
    except:
        print 'Cannot open output file: ' + accBCPFile
        sys.exit(1)

    return


#
# Purpose: Close the files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def closeFiles ():
    fpInputFile.close()
    fpRptFile.close()
    fpAccBCPFile.close()

    return


#
# Purpose: Create the discrepancy report for the GEO data.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createReport ():
    global badIDs

    print 'Create the discrepancy report'
    fpRptFile.write(26*' ' + 'GEO Discrepancy Report' + NL)
    fpRptFile.write(24*' ' + '(' + timestamp + ')' + 2*NL)
    fpRptFile.write('%-15s  %-75s%s' % ('EntrezGene ID','Discrepancy',NL))
    fpRptFile.write(15*'-' + '  ' + 75*'-' + NL)

    cmds = []

    #
    # Find any cases where the EntrezGene ID from the input file is not
    # associated with a marker.
    #
    cmds.append('select t.entrezgeneID ' + \
                'into #not_in_mgi ' + \
                'from tempdb..' + tempTable + ' t ' + \
                'where not exists (select 1 ' + \
                                  'from ACC_Accession a ' + \
                                  'where a.accID = t.entrezgeneID and ' + \
                                        'a._MGIType_key = ' + str(markerMGITypeKey) + ' and ' + \
                                        'a._LogicalDB_key = ' + str(egLogicalDBKey) + ')')

    cmds.append('select entrezgeneID from #not_in_mgi ' + \
                'order by entrezgeneID')

    #
    # Find any cases where the EntrezGene ID from the input file is
    # associated with more than one marker.
    #
    cmds.append('select t.entrezgeneID ' + \
                'into #many_marker ' + \
                'from tempdb..' + tempTable + ' t, ACC_Accession a ' + \
                'where t.entrezgeneID = a.accID and ' + \
                      'a._MGIType_key = ' + str(markerMGITypeKey) + ' and ' + \
                      'a._LogicalDB_key = ' + str(egLogicalDBKey) + ' ' + \
                'group by t.entrezgeneID ' + \
                'having count(*) > 1')

    cmds.append('select entrezgeneID from #many_marker ' + \
                'order by entrezgeneID')

    #
    # Find any cases where the EntrezGene ID from the input file is
    # associated with a marker that is associated with multiple EntrezGene IDs.
    #
    cmds.append('select a._Object_key ' + \
                'into #markers ' + \
                'from ACC_Accession a ' + \
                'where a._MGIType_key = ' + str(markerMGITypeKey) + ' and ' + \
                      'a._LogicalDB_key = ' + str(egLogicalDBKey) + ' ' + \
                'group by a._Object_key ' + \
                'having count(*) > 1')

    cmds.append('select t.entrezgeneID ' + \
                'into #many_eg ' + \
                'from tempdb..' + tempTable + ' t, ACC_Accession a, #markers m ' + \
                'where t.entrezgeneID = a.accID and ' + \
                      'a._MGIType_key = ' + str(markerMGITypeKey) + ' and ' + \
                      'a._LogicalDB_key = ' + str(egLogicalDBKey) + ' and ' + \
                      'a._Object_key = m._Object_key')

    cmds.append('select distinct entrezgeneID from #many_eg ' + \
                'order by entrezgeneID')

    results = db.sql(cmds,'auto')

    badIDs = {}
    count = 0

    #
    # Write the records to the discrepancy report.  Keep track of the
    # EntrezGene IDs that have discrepancies, so associations are not
    # made for them later.
    #
    for r in results[1]:
        fpRptFile.write('%-15s  %-75s%s' %
            (r['entrezgeneID'], 'EntrezGene ID not associated with a marker', NL))
        if not badIDs.has_key(r['entrezgeneID']):
            badIDs[r['entrezgeneID']] = r['entrezgeneID']
        count = count + 1

    for r in results[3]:
        fpRptFile.write('%-15s  %-75s%s' %
            (r['entrezgeneID'], 'EntrezGene ID associated with multiple markers', NL))
        if not badIDs.has_key(r['entrezgeneID']):
            badIDs[r['entrezgeneID']] = r['entrezgeneID']
        count = count + 1

    for r in results[6]:
        fpRptFile.write('%-15s  %-75s%s' %
            (r['entrezgeneID'], 'EntrezGene ID associated with a marker that has multiple EG associations', NL))

        if not badIDs.has_key(r['entrezgeneID']):
            badIDs[r['entrezgeneID']] = r['entrezgeneID']
        count = count + 1

    fpRptFile.write(NL + 'Number of discrepancies: ' + str(count) + NL)

    print 'Number of discrepancies: ' + str(count)

    return


#
# Purpose: Create the bcp file for the GEO associations.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createBCPFile ():
    global accKey

    print 'Create the bcp file for the GEO associations'

    #
    # Find the marker key that the EntrezGene ID should be associated with.
    # Do not make an association for any EntrezGene IDs that are on the
    # discrepancy report.
    #
    cmds = []
    cmds.append('select t.entrezgeneID, a._Object_key "markerKey" ' + \
                'from tempdb..' + tempTable + ' t, ACC_Accession a ' + \
                'where t.entrezgeneID = a.accID and ' + \
                      'a._MGIType_key = ' + str(markerMGITypeKey) + ' and ' + \
                      'a._LogicalDB_key = ' + str(egLogicalDBKey) + ' ' + \
                'order by t.entrezgeneID')

    results = db.sql(cmds,'auto')

    count = 0

    #
    # Write the records to the bcp file.
    #
    for r in results[0]:
        entrezgeneID = r['entrezgeneID']
        markerKey = r['markerKey']

        #
        # Skip the EntrezGene ID if it was written to the discrepancy report.
        #
        if badIDs.has_key(entrezgeneID):
            continue

        #
        # Get the prefix and numeric parts of the EntrezGene ID and write
        # a record to the bcp file.
        #
        (prefixPart,numericPart) = accessionlib.split_accnum(entrezgeneID)

        fpAccBCPFile.write(str(accKey) + TAB + \
                           entrezgeneID + TAB + \
                           prefixPart + TAB + \
                           str(numericPart) + TAB + \
                           str(geoLogicalDBKey) + TAB + \
                           str(markerKey) + TAB + \
                           str(markerMGITypeKey) + TAB + \
                           PRIVATE + TAB + PREFERRED + TAB + \
                           str(createdByKey) + TAB + \
                           str(createdByKey) + TAB + \
                           loadDate + TAB + \
                           loadDate + NL)

        count = count + 1
        accKey = accKey + 1

    print 'Number of GEO associations: ' + str(count)

    return


#
# Main
#
init()
openFiles()
createReport()
createBCPFile()
closeFiles()

sys.exit(0)
