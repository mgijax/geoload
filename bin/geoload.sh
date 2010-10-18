#!/bin/sh
#
#  geoload.sh
###########################################################################
#
#  Purpose:
#
#      This script serves as a wrapper for the geoload.py script.
#
#  Usage:
#
#      geoload.sh
#
#
#  Env Vars:
#
#      See the configuration files
#
#  Inputs:  None
#
#  Outputs:
#
#      - Generated Input File (${GEOLOAD_INPUTFILE}) containing a list of
#        EntrezGene IDs
#
#      - Log file (${GEOLOAD_LOGFILE})
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  Fatal error occurred
#
#  Assumes:  Nothing
#
#  Implementation:
#
#      This script will perform following steps:
#
#      1) Call the Python script (getGeoEntrez.py) to generate the input file
#         for the load by invoking the GEO query tool.
#      2) Create the temp table for the input data.
#      3) Load the input file into the temp table.
#      4) Call the Python script (geoload.py) to create a bcp file with
#         GEO associations and a discrepancy report for input records that
#         could not be processed.
#      5) Drop the temp table.
#      6) Delete the existing GEO associations.
#      7) Load the bcp file into the ACC_Accession table to establish the
#         new GEO associations.
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

cd `dirname $0`
. ../Configuration

LOG=${GEOLOAD_LOGFILE}
rm -rf ${LOG}
touch ${LOG}

#
# Create a temporary file that will hold the return code from calling the
# Python script.  Make sure the file is removed when this script terminates.
#
TMP_RC=/tmp/`basename $0`.$$
trap "rm -f ${TMP_RC}" 0 1 2 15

#
# Generate the input file for the load.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Generate the input file for the load" | tee -a ${LOG}
{ ./getGeoEntrez.py 2>&1; echo $? > ${TMP_RC}; } >> ${LOG}
if [ `cat ${TMP_RC}` -ne 0 ]
then
    echo "GEO load failed" | tee -a ${LOG}
    exit 1
elif [ ! -s ${GEOLOAD_INPUTFILE} ]
then
    echo "The GEO load input file is empty" | tee -a ${LOG}
    exit 1
fi

#
# Create the temp table for the input data.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Create the temp table (${GEO_TEMP_TABLE}) for the input data" | tee -a ${LOG}
cat - <<EOSQL | isql -S${MGD_DBSERVER} -D${MGD_DBNAME} -Umgd_dbo -P`cat ${MGD_DBPASSWORDFILE}` -e  >> ${LOG}

use tempdb
go

create table ${GEO_TEMP_TABLE} (
    entrezgeneID varchar(30) not null
)
go

create nonclustered index idx_entrezgeneID on ${GEO_TEMP_TABLE} (entrezgeneID)
go

grant all on ${GEO_TEMP_TABLE} to public
go

quit
EOSQL

#
# Load the input file into the temp table.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Load the input file into the temp table" | tee -a ${LOG}
cat ${MGD_DBPASSWORDFILE} | bcp tempdb..${GEO_TEMP_TABLE} in ${GEOLOAD_INPUTFILE} -c -t\\t -S${MGD_DBSERVER} -U${MGD_DBUSER} >> ${LOG}

#
# Create the GEO association file and discrepancy report.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Create the GEO association file and discrepancy report" | tee -a ${LOG}
{ ./geoload.py 2>&1; echo $? > ${TMP_RC}; } >> ${LOG}
if [ `cat ${TMP_RC}` -ne 0 ]
then
    echo "GEO load failed" | tee -a ${LOG}
    QUIT=1
elif [ ! -s ${GEOLOAD_ACC_BCPFILE} ]
then
    echo "The association file is empty" | tee -a ${LOG}
    QUIT=1
else
    QUIT=0
fi

#
# Drop the temp table.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Drop the temp table (${GEO_TEMP_TABLE})" | tee -a ${LOG}
cat - <<EOSQL | isql -S${MGD_DBSERVER} -D${MGD_DBNAME} -Umgd_dbo -P`cat ${MGD_DBPASSWORDFILE}` -e  >> ${LOG}

use tempdb
go

drop table ${GEO_TEMP_TABLE}
go

quit
EOSQL

#
# Do not attempt to delete/reload the GEO associations if there was a
# problem creating the assocation file.
#
if [ ${QUIT} -eq 1 ]
then
    exit 1
fi

#
# Delete the existing GEO associations.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Delete the existing GEO associations" | tee -a ${LOG}
cat - <<EOSQL | isql -S${MGD_DBSERVER} -D${MGD_DBNAME} -Umgd_dbo -P`cat ${MGD_DBPASSWORDFILE}` -e >> ${LOG}

declare @logicalDBKey int
select @logicalDBKey = _LogicalDB_key
from ACC_LogicalDB
where name = 'GEO'

delete from ACC_Accession
where _LogicalDB_key = @logicalDBKey
go

quit
EOSQL

#
# Load the new GEO associations.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Load the new GEO associations" | tee -a ${LOG}
cat ${MGD_DBPASSWORDFILE} | bcp ${MGD_DBNAME}..ACC_Accession in ${GEOLOAD_ACC_BCPFILE} -c -t\\t -S${MGD_DBSERVER} -U${MGD_DBUSER} >> ${LOG}

date >> ${LOG}

exit 0
