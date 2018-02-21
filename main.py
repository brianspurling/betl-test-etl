from config import CONFIG
import betl
import df_transform_a_ods as df_transform_ods
import df_transform_b_mrg as df_transform_mrg
import df_transform_c_trg_dm as df_transform_dm
import df_transform_d_trg_ft as df_transform_ft
import df_extract

import sys

betl.processArgs(sys.argv)

##################
# Configure betl #
##################

betl.loadAppConfig(CONFIG)

########################
# Extract Job Schedule #
########################

betl.addDefaultExtractToSchedule(srcTablesToExclude=['src_wp_documents'])
betl.scheduleDataFlow(df_extract.extractPosts, 'EXTRACT')

##########################
# Transform Job Schedule #
##########################

# Prep #

# ODS #

# Load the ODS data model

betl.scheduleDataFlow(df_transform_ods.loadCompaniesToODS, 'TRANSFORM')
betl.scheduleDataFlow(df_transform_ods.loadPeopleToODS, 'TRANSFORM')
betl.scheduleDataFlow(df_transform_ods.loadAddressesToODS, 'TRANSFORM')
betl.scheduleDataFlow(df_transform_ods.loadPostsToODS, 'TRANSFORM')
betl.scheduleDataFlow(df_transform_ods.loadLinksToODS, 'TRANSFORM')

# Load the MRG data model # -- Note, doesn't have a persistent layer

betl.scheduleDataFlow(df_transform_mrg.loadCompaniesToMRG, 'TRANSFORM')
betl.scheduleDataFlow(df_transform_mrg.loadPeopleToMRG, 'TRANSFORM')
betl.scheduleDataFlow(df_transform_mrg.loadAddressesToMRG, 'TRANSFORM')
betl.scheduleDataFlow(df_transform_mrg.loadLinksToMRG, 'TRANSFORM')

# Dimensions #

# Prepare dimension staging datasets

betl.scheduleDataFlow(df_transform_dm.prepareDMLinkType, 'TRANSFORM')
betl.scheduleDataFlow(df_transform_dm.prepareDMRelationship, 'TRANSFORM')
betl.scheduleDataFlow(df_transform_dm.prepareDMNode, 'TRANSFORM')
betl.scheduleDataFlow(df_transform_dm.prepareDMCorruptionDoc, 'TRANSFORM')
betl.scheduleDataFlow(df_transform_dm.prepareDMAddress, 'TRANSFORM')
betl.scheduleDataFlow(df_transform_dm.prepareDMAddressType, 'TRANSFORM')
betl.scheduleDataFlow(df_transform_dm.prepareDMNetworkMetric, 'TRANSFORM')

# DM date
betl.addDMDateToSchedule()

# Facts #

# Prepare fact staging datasets

betl.scheduleDataFlow(df_transform_ft.prepareFTLinks, 'TRANSFORM')

#####################
# Load Job Schedule #
#####################

nonDefaultStagingTables = {
    'ft_mentions': 'skip this one',
    'ft_addresses_nodes': 'skip this one',
    'ft_network_metrics': 'skip this one'
}
betl.addDefaultLoadToSchedule(nonDefaultStagingTables)

###################
# Execute the Job #
###################

# Note, this doesn't necessarily run the ETL job; it depends on sys.argv
# parameters passed in on the command line
betl.run()
