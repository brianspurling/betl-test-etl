import sys

import betl

import df_transform_a_ods as df_transform_ods
import df_transform_b_mrg as df_transform_mrg
import df_transform_c_trg_dm as df_transform_dm
import df_transform_d_trg_ft as df_transform_ft
import df_extract
import df_load

##################
# Configure betl #
##################

scheduleConfig = {
    'DEFAULT_EXTRACT': True,
    'SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXTRACT': [
        'src_wp_documents'
    ],
    'DEFAULT_LOAD': True,
    'DEFAULT_DM_DATE': True,
    'TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD': [
        'ft_mentions',
        'ft_addresses_nodes',
        'ft_network_metrics'
    ],
    'EXTRACT_DFS': [
        df_extract.extractPosts
    ],
    'TRANSFORM_DFS': [
        df_transform_ods.loadCompaniesToODS,
        df_transform_ods.loadPeopleToODS,
        df_transform_ods.loadAddressesToODS,
        df_transform_ods.loadPostsToODS,
        df_transform_ods.loadSrcLinksToODS,
        df_transform_mrg.loadCompaniesToMRG,
        df_transform_mrg.loadPeopleToMRG,
        df_transform_mrg.loadAddressesToMRG,
        df_transform_mrg.loadSrcLinksToMRG,
        df_transform_dm.prepareDMLinkType,
        df_transform_dm.prepareDMRelationship,
        df_transform_dm.prepareDMNode,
        df_transform_dm.prepareDMCorruptionDoc,
        df_transform_dm.prepareDMAddress,
        df_transform_dm.prepareDMAddressType,
        df_transform_dm.prepareDMNetworkMetric,
        df_transform_ft.loadSrcLinksIntoTempFile,
        df_transform_ft.generateLinks_C2P,
        df_transform_ft.generateLinks_P2P_prep,
        df_transform_ft.generateLinks_P2P_where,
        df_transform_ft.generateLinks_P2P_while,
        df_transform_ft.prepareFTLinks
    ],
    'LOAD_DFS': [
        df_load.createTextSearchColumns
    ],
}

betl.run(appConfigFile='./appConfig.ini',
         runTimeParams=betl.processArgs(sys.argv),
         scheduleConfig=scheduleConfig)
