from . import df_extract_posts as df_extract_posts
from . import df_transform_a_ods as df_transform_ods
from . import df_transform_b_mrg as df_transform_mrg
from . import df_transform_c_bse_dm as df_transform_dm
from . import df_transform_d_bse_ft as df_transform_ft
from . import df_load_search_cols as df_load
from . import df_summarise as df_summarise

# TODO: must use relative user path (~/) and / at end
# Is it possible to auto set them??
APP_DIRECTORY = '~/git/pngi/pngi/'

APP_CONFIG_FILENAME = 'appConfig.ini'

SCHEDULE_CONFIG = {
    'DEFAULT_EXTRACT': True,
    'DEFAULT_TRANSFORM': True,
    'DEFAULT_LOAD': True,
    'DEFAULT_SUMMARISE': True,
    'DEFAULT_DM_DATE': True,
    'DEFAULT_DM_AUDIT': True,
    'EXT_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT': ['wp_documents'],
    'BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD': [
        'ft_addresses_nodes',
        'ft_network_metrics'],
    # Bespoke extract DFs are run in parallel with the default extract DF
    'EXTRACT_DATAFLOWS': {
        'extractPosts': {'func': df_extract_posts.extractPosts},
    },
    'TRANSFORM_DATAFLOWS': {
        'loadCompaniesToODS': {
            'func': df_transform_ods.loadCompaniesToODS},
        'loadPeopleToODS': {
            'func': df_transform_ods.loadPeopleToODS},
        'loadAddressesToODS': {
            'func': df_transform_ods.loadAddressesToODS},
        'loadPostsToODS': {
            'func': df_transform_ods.loadPostsToODS},
        'loadSrcLinksToODS': {
            'func': df_transform_ods.loadSrcLinksToODS,
            'upstream': ['loadPeopleToODS', 'loadCompaniesToODS']},
        'loadCompaniesToMRG': {
            'func': df_transform_mrg.loadCompaniesToMRG,
            'upstream': ['loadCompaniesToODS']},
        'loadPeopleToMRG': {
            'func': df_transform_mrg.loadPeopleToMRG,
            'upstream': ['loadPeopleToODS']},
        'loadAddressesToMRG': {
            'func': df_transform_mrg.loadAddressesToMRG,
            'upstream': ['loadAddressesToODS']},
        'loadSrcLinksToMRG': {
            'func': df_transform_mrg.loadSrcLinksToMRG,
            'upstream': ['loadSrcLinksToODS']},
        'prepareDMLinkType': {
            'func': df_transform_dm.prepareDMLinkType},
        'prepareDMRelationship': {
            'func': df_transform_dm.prepareDMRelationship},
        'prepareDMNode': {
            'func': df_transform_dm.prepareDMNode,
            'upstream': ['loadCompaniesToMRG', 'loadPeopleToMRG']},
        'prepareDMCorruptionDoc': {
            'func': df_transform_dm.prepareDMCorruptionDoc,
            'upstream': ['loadPostsToODS']},
        'prepareDMAddress': {
            'func': df_transform_dm.prepareDMAddress,
            'upstream': ['loadAddressesToMRG']},
        'prepareDMAddressType': {
            'func': df_transform_dm.prepareDMAddressType,
            'upstream': ['loadAddressesToMRG']},
        'prepareDMNetworkMetric': {
            'func': df_transform_dm.prepareDMNetworkMetric},
        'generateLinks_C2P': {
            'func': df_transform_ft.generateLinks_C2P,
            'upstream': ['loadSrcLinksToMRG']},
        'generateLinks_P2P_prep': {
            'func': df_transform_ft.generateLinks_P2P_prep,
            'upstream': ['loadSrcLinksToMRG']},
        'generateLinks_P2P_where': {
            'func': df_transform_ft.generateLinks_P2P_where,
            'upstream': ['generateLinks_P2P_prep']},
        'generateLinks_P2P_while': {
            'func': df_transform_ft.generateLinks_P2P_while,
            'upstream': ['generateLinks_P2P_prep']},
        'prepareFTLinks': {
            'func': df_transform_ft.prepareFTLinks,
            'upstream': [
                'generateLinks_C2P',
                'generateLinks_P2P_prep',
                'generateLinks_P2P_where',
                'generateLinks_P2P_while']},
    },
    # Bespoke load DFs are run after their respective default load DFs
    'LOAD_DIM_DATAFLOWS': {
        'createTextSearchColumns': {
            'func': df_load.createTextSearchColumns}
    },
    'LOAD_FACT_DATAFLOWS': {},
    # Bespoke summarise DFs are run in between the default prep & finish DFs
    'SUMMARISE_DATAFLOWS': {
        'buildSuMentions': {'func': df_summarise.buildSuMentions},
        'writeBackMentions': {
            'func': df_summarise.writeBackMentions,
            'upstream': ['buildSuMentions']}
    }
}
