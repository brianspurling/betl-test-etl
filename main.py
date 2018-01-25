from config import CONFIG
import betl
import df_transform
import df_extract

import sys

betl.processArgs(sys.argv)

# Configure betl
betl.loadAppConfig(CONFIG)

# Configure job "extras"
# betl.scheduleDataFlow(betl.profileSrc, 'EXTRACT')

#
# Set up our job schedule
#

# Extract #

betl.addDefaultExtractToSchedule(srcTablesToExclude=['src_wp_documents'])
betl.scheduleDataFlow(df_extract.extractPosts, 'EXTRACT')

# Transform #

# Get data into ODS
betl.scheduleDataFlow(df_transform.getAllCompanies, 'TRANSFORM')
betl.scheduleDataFlow(df_transform.getAllPeople, 'TRANSFORM')
betl.scheduleDataFlow(df_transform.getAllAddresses, 'TRANSFORM')

# Dedupe the data in MRG
betl.scheduleDataFlow(df_transform.dedupeCompanies, 'TRANSFORM')
betl.scheduleDataFlow(df_transform.dedupePeople, 'TRANSFORM')
betl.scheduleDataFlow(df_transform.dedupeAddresses, 'TRANSFORM')

# betl.scheduleDataFlow(df_transform.prepareDMNode, 'TRANSFORM')

# betl.addDMDateToSchedule()

# Execute the job (note, this doesn't necessarily run the ETL job;
# it depends on sys.argv parameters)
betl.run()
