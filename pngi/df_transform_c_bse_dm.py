def prepareDMLinkType(betl):

    dfl = betl.DataFlow(desc='Prep msd_dm_link_type data for default load')

    dfl.read(tableName='msd_dm_link_type', dataLayer='EXT')

    dfl.prepForLoad(dataset='msd_dm_link_type',
                    targetTableName='dm_link_type')


def prepareDMRelationship(betl):

    dfl = betl.DataFlow(desc='Prep msd_dm_relationship data for ' +
                             'default load')

    dfl.read(tableName='msd_dm_relationship', dataLayer='EXT')

    dfl.prepForLoad(
        dataset='msd_dm_relationship',
        targetTableName='dm_relationship')


def prepareDMNode(betl):

    dfl = betl.DataFlow(desc='Prep dm_node data for default load')

    dfl.read(tableName='mrg_companies', dataLayer='TRN')

    dfl.renameColumns(
        dataset='mrg_companies',
        columns={'company_name_cleaned': 'name'},
        desc='Rename company_name_cleaned to name')

    dfl.addColumns(
        dataset='mrg_companies',
        columns={'node_type': 'company'},
        desc='Add column: node_type = company')

    dfl.read(tableName='mrg_people', dataLayer='TRN')

    dfl.renameColumns(
        dataset='mrg_people',
        columns={'person_name_cleaned': 'name'},
        desc='Rename person_name_cleaned to name')

    dfl.addColumns(
        dataset='mrg_people',
        columns={'node_type': 'person'},
        desc='Add column: node_type = person')

    dfl.union(
        datasets=['mrg_companies', 'mrg_people'],
        targetDataset='dm_node',
        desc='Union the two datasets')

    dfl.applyFunctionToColumns(
        dataset='dm_node',
        function=alphnumericName,
        columns='name',
        targetColumns='name_alphanumeric',
        desc="Create an alphanumeric-only name")

    dfl.addColumns(
        dataset='dm_node',
        columns={'name_tsquery': None,
                 'is_mentioned_in_docs': None,
                 'mentions_count': None},
        desc='Add empty columns (to be populated later)')

    dfl.prepForLoad(dataset='dm_node')


def prepareDMCorruptionDoc(betl):

    dfl = betl.DataFlow(desc='Prep dm_corruption_doc data for default load')

    dfl.read(tableName='ods_posts', dataLayer='TRN')

    dfl.applyFunctionToColumns(
        dataset='ods_posts',
        function=alphnumericName,
        columns='corruption_doc_content',
        targetColumns='corruption_doc_content_alphanumeric',
        desc="Create alphanumeric-only content")

    dfl.addColumns(
        dataset='ods_posts',
        columns={'number_mentioned_nodes': 0,
                 'number_mentioned_people': 0,
                 'number_mentioned_companies': 0,
                 'corruption_doc_tsvector': None},
        desc='Add 4 columns: number_mentioned_*, and tsvector, ' +
             ' (to be populated later)')

    dfl.prepForLoad(
        dataset='ods_posts',
        targetTableName='dm_corruption_doc')


def prepareDMAddress(betl):

    dfl = betl.DataFlow(desc='Prep dm_addresses data for default load')

    dfl.read(tableName='mrg_addresses', dataLayer='TRN')

    dfl.renameColumns(
        dataset='mrg_addresses',
        columns={'address_cleaned': 'address'},
        desc='Rename column: address to address_cleaned')

    dfl.prepForLoad(
        dataset='mrg_addresses',
        targetTableName='dm_address')


def prepareDMAddressType(betl):

    dfl = betl.DataFlow(
        desc='Prep msd_dm_address_type and ods_addresses data for ' +
             'default load of dm_address_type')

    # ODS_ADDRESSES

    dfl.read(tableName='ods_addresses', dataLayer='TRN')

    dfl.dropColumns(
        dataset='ods_addresses',
        colsToKeep=['address_type'],
        desc='Drop all cols except address_type')

    dfl.dedupe(dataset='ods_addresses',
               desc='Dedupe address_types')

    dfl.addColumns(
        dataset='ods_addresses',
        columns={'address_role': 'Company',
                 'address_type_name': addressTypeName},
        desc='Add address_role column')

    # MSD ADDRESS_TYPE

    dfl.read(tableName='msd_dm_address_type', dataLayer='EXT')

    # DM_ADDRESS_TYPE

    dfl.union(
        datasets=['ods_addresses', 'msd_dm_address_type'],
        targetDataset='dm_address_type',
        desc='Union the two datasets')

    dfl.prepForLoad(dataset='dm_address_type')


def prepareDMNetworkMetric(betl):

    dfl = betl.DataFlow(
        desc='Prep msd_dm_network_metric data for default load')

    dfl.read(tableName='msd_dm_network_metric', dataLayer='EXT')

    dfl.prepForLoad(
        dataset='msd_dm_network_metric',
        targetTableName='dm_network_metric')


####################
# UTILITY FUNCTIONS #
#####################

def alphnumericName(colToClean):
    return colToClean.str.replace('[^[:alnum:] ]', ' ')


def addressTypeName(row):
    return 'company: ' + row['address_type']
