import betl


def prepareDMLinkType(scheduler):

    dfl = betl.DataFlow(desc='Prep src_msd_dm_link_type data for default load')

    dfl.read(tableName='src_msd_dm_link_type', dataLayer='SRC')

    dfl.write(
        dataset='src_msd_dm_link_type',
        targetTableName='trg_dm_link_type',
        dataLayerID='STG')

    dfl.close()


def prepareDMRelationship(scheduler):

    dfl = betl.DataFlow(desc='Prep src_msd_dm_relationship data for ' +
                             'default load')

    dfl.read(tableName='src_msd_dm_relationship', dataLayer='SRC')

    dfl.write(
        dataset='src_msd_dm_relationship',
        targetTableName='trg_dm_relationship',
        dataLayerID='STG')

    dfl.close()


def prepareDMNode(scheduler):

    dfl = betl.DataFlow(desc='Prep dm_node data for default load')

    dfl.read(tableName='mrg_companies', dataLayer='STG')

    dfl.renameColumns(
        dataset='mrg_companies',
        columns={'company_name_cleaned': 'name'},
        desc='Rename company_name_cleaned to name')

    dfl.addColumns(
        dataset='mrg_companies',
        columns={'node_type': 'company'},
        desc='Add column: node_type = company')

    dfl.read(tableName='mrg_people', dataLayer='STG')

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
        targetDataset='trg_dm_node',
        desc='Union the two datasets')

    dfl.cleanColumn(
        dataset='trg_dm_node',
        cleaningFunc=alphnumericName,
        column='name',
        cleanedColumn='name_alphanumeric',
        desc="Create an alphanumeric-only name")

    dfl.addColumns(
        dataset='trg_dm_node',
        columns={'name_tsquery': None,
                 'is_mentioned_in_docs': None,
                 'mentions_count': None},
        desc='Add empty columns (to be populated later)')

    dfl.write(
        dataset='trg_dm_node',
        targetTableName='trg_dm_node',
        dataLayerID='STG')

    dfl.close()


def prepareDMCorruptionDoc(scheduler):

    dfl = betl.DataFlow(desc='Prep dm_corruption_doc data for default load')

    dfl.read(tableName='ods_posts', dataLayer='STG')

    dfl.cleanColumn(
        dataset='ods_posts',
        cleaningFunc=alphnumericName,
        column='corruption_doc_content',
        cleanedColumn='corruption_doc_content_alphanumeric',
        desc="Create alphanumeric-only content")

    dfl.addColumns(
        dataset='ods_posts',
        columns={'number_mentioned_nodes': 0,
                 'number_mentioned_people': 0,
                 'number_mentioned_companies': 0,
                 'corruption_doc_tsvector': None},
        desc='Add 4 columns: number_mentioned_*, and tsvector, ' +
             ' (to be populated later)')

    dfl.write(
        dataset='ods_posts',
        targetTableName='trg_dm_corruption_doc',
        dataLayerID='STG')

    dfl.close()


def prepareDMAddress(scheduler):

    dfl = betl.DataFlow(desc='Prep dm_addresses data for default load')

    dfl.read(tableName='mrg_addresses', dataLayer='STG')

    dfl.renameColumns(
        dataset='mrg_addresses',
        columns={'address_cleaned': 'address'},
        desc='Rename column: address to address_cleaned')

    dfl.write(
        dataset='mrg_addresses',
        targetTableName='trg_dm_address',
        dataLayerID='STG')

    dfl.close()


def prepareDMAddressType(scheduler):

    dfl = betl.DataFlow(
        desc='Prep src_msd_dm_address_type and ods_addresses data for ' +
             'default load of dm_address_type')

    colList = ['address_type_name',
               'address_type',
               'address_role']

    # ODS_ADDRESSES

    dfl.read(tableName='ods_addresses', dataLayer='STG')

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

    dfl.sortColumns(
        dataset='ods_addresses',
        colList=colList,
        desc='Sort list ready for union')

    # MSD ADDRESS_TYPE

    dfl.read(tableName='src_msd_dm_address_type', dataLayer='SRC')

    dfl.sortColumns(
        dataset='src_msd_dm_address_type',
        colList=colList,
        desc='Sort list ready for union')

    # TRG_DM_ADDRESS_TYPE

    dfl.union(
        datasets=['ods_addresses', 'src_msd_dm_address_type'],
        targetDataset='trg_dm_address_type',
        desc='Union the two datasets')

    dfl.write(
        dataset='trg_dm_address_type',
        targetTableName='trg_dm_address_type',
        dataLayerID='STG')

    dfl.close()


def prepareDMNetworkMetric(scheduler):

    dfl = betl.DataFlow(
        desc='Prep src_msd_dm_network_metric data for default load')

    dfl.read(tableName='src_msd_dm_network_metric', dataLayer='SRC')

    dfl.write(
        dataset='src_msd_dm_network_metric',
        targetTableName='trg_dm_network_metric',
        dataLayerID='STG')

    dfl.close()


####################
# UTILITY FUNCTIONS #
#####################

def alphnumericName(colToClean):
    return colToClean.str.replace('[^[:alnum:] ]', ' ')


def addressTypeName(row):
    return 'company: ' + row['address_type']
