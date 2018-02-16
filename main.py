from config import CONFIG
import betl
import df_transform
import df_extract

import sys

betl.processArgs(sys.argv)

# Configure betl
betl.loadAppConfig(CONFIG)

# Profiling
# betl.scheduleDataFlow(betl.profileSrc, 'EXTRACT')

#
# Set up our job schedule
#

# Extract #

betl.addDefaultExtractToSchedule(srcTablesToExclude=['src_wp_documents'])
betl.scheduleDataFlow(df_extract.extractPosts, 'EXTRACT')

# Transform #

# Get data
betl.scheduleDataFlow(df_transform.getCompanies, 'TRANSFORM')
betl.scheduleDataFlow(df_transform.getPeople, 'TRANSFORM')
betl.scheduleDataFlow(df_transform.getAddresses, 'TRANSFORM')

# Load the ODS data model
betl.scheduleDataFlow(df_transform.loadCompaniesToODS, 'TRANSFORM')
betl.scheduleDataFlow(df_transform.loadPeopleToODS, 'TRANSFORM')
betl.scheduleDataFlow(df_transform.loadAddressesToODS, 'TRANSFORM')
betl.scheduleDataFlow(df_transform.loadPostsToODS, 'TRANSFORM')

# Dedupe the data [not currently writing to a MRG data model]
betl.scheduleDataFlow(df_transform.dedupeCompanies, 'TRANSFORM')
betl.scheduleDataFlow(df_transform.dedupePeople, 'TRANSFORM')
betl.scheduleDataFlow(df_transform.dedupeAddresses, 'TRANSFORM')

# Prepare the data for load
betl.scheduleDataFlow(df_transform.prepareDMLinkType, 'TRANSFORM')
betl.scheduleDataFlow(df_transform.prepareDMRelationship, 'TRANSFORM')
betl.scheduleDataFlow(df_transform.prepareDMNode, 'TRANSFORM')
betl.scheduleDataFlow(df_transform.prepareDMCorruptionDoc, 'TRANSFORM')
betl.scheduleDataFlow(df_transform.prepareDMAddress, 'TRANSFORM')
betl.scheduleDataFlow(df_transform.prepareDMAddressType, 'TRANSFORM')
betl.scheduleDataFlow(df_transform.prepareDMNetworkMetric, 'TRANSFORM')

betl.addDMDateToSchedule()

nonDefaultStagingTables = {
    'ft_links': 'skip this one',
    'ft_mentions': 'skip this one',
    'ft_addresses_nodes': 'skip this one',
    'ft_network_metrics': 'skip this one'
}
# betl.addDefaultLoadToSchedule(nonDefaultStagingTables)

# Execute the job (note, this doesn't necessarily run the ETL job;
# it depends on sys.argv parameters)
betl.run()
