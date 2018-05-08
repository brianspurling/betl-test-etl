import betl
import datetime

# TODO: having this as a global this breaks recovery mid-execution
COL_LIST = None


def generateLinks_C2P(scheduler):

    global COL_LIST

    dfl = betl.DataFlow(
        desc='Generate the company-to-person (C2P) links by flipping the ' +
             'P2C links')

    dfl.read(tableName='mrg_src_links', dataLayer='STG')

    dfl.addColumns(
        dataset='mrg_src_links',
        columns={
            'commonality_node_cleaned': None,
            'commonality_node_type': None},
        desc='Add commonality node columns (to be populated later)')

    COL_LIST = dfl.getColumnList('mrg_src_links')

    dfl.renameColumns(
        dataset='mrg_src_links',
        columns={
            'origin_node_cleaned': 'target_node_cleaned',
            'origin_node_type': 'target_node_type',
            'target_node_cleaned': 'origin_node_cleaned',
            'target_node_type': 'origin_node_type',
        },
        desc='Rename cols to swap origin/target over and reorder')

    dfl.setColumns(
        dataset='mrg_src_links',
        columns={'link_type': setLinkType},
        desc='Set value of link_type column by flipping C2P & P2C')

    dfl.cleanColumn(
        dataset='mrg_src_links',
        cleaningFunc=setRelationship,
        column='relationship',
        desc="Set the value of the relationship column by flipping of & by")

    dfl.write(
        dataset='mrg_src_links',
        targetTableName='tmp_ft_links_generated_C2P',
        dataLayerID='STG')


def generateLinks_P2P_prep(scheduler):

    dfl = betl.DataFlow(
        desc='Prep for the generation of person-to-person (P2P) links ' +
             'by flipping the writing all P2C links to a temp DB table')

    dfl.read(tableName='mrg_src_links', dataLayer='STG')

    dfl.addColumns(
        dataset='mrg_src_links',
        columns={
            'commonality_node_cleaned': None,
            'commonality_node_type': None},
        desc='Add commonality node columns (to be populated later)')

    dfl.filter(
        dataset='mrg_src_links',
        filters={'link_type': 'P2C'},
        desc='Filter to P2C links only')

    dfl.write(
        dataset='mrg_src_links',
        targetTableName='tmp_ft_links_src_grouped',
        dataLayerID='STG',
        forceDBWrite=True,
        # TODO: ideally the framework would take care of this
        dtype={'start_date': 'text'},
        desc='Forcing this write to DB because the next step is custom SQL')


def generateLinks_P2P_where(scheudler):

    sql = ''
    sql += "SELECT LEAST(l1.start_date, l2.start_date) AS start_date, \n"
    sql += "       GREATEST(l1.end_date, l2.end_date) AS end_date, \n"
    sql += "       l1.origin_node_cleaned, \n"
    sql += "       l1.origin_node_type, \n"
    sql += "       l2.origin_node_cleaned AS target_node_cleaned, \n"
    sql += "       l2.origin_node_type AS target_node_type, \n"
    sql += "       l1.target_node_cleaned AS commonality_node_cleaned, \n"
    sql += "       l1.target_node_type AS commonality_node_type, \n"
    sql += "	   'P2P' AS link_type, \n"
    sql += "	   substring(l1.relationship from 1 for \n"
    sql += "                 position('_' in l1.relationship)-1) \n"
    sql += "       || '=' || \n"
    sql += "       substring(l2.relationship from 1 for \n"
    sql += "                 position('_' in l2.relationship)-1) \n"
    sql += "       AS relationship \n"
    sql += "FROM   (SELECT origin_node_cleaned, \n"
    sql += "			   origin_node_type, \n"
    sql += "			   target_node_cleaned, \n"
    sql += "			   target_node_type, \n"
    sql += "			   link_type, \n"
    sql += "			   relationship, \n"
    sql += "			   start_date, \n"
    sql += "			   end_date \n"
    sql += " 	   FROM    tmp_ft_links_src_grouped) AS l1 \n"
    sql += "LEFT JOIN (SELECT origin_node_cleaned, \n"
    sql += "			      origin_node_type, \n"
    sql += "			      target_node_cleaned, \n"
    sql += "			      target_node_type, \n"
    sql += "			      link_type, \n"
    sql += "			      relationship, \n"
    sql += "			      start_date, \n"
    sql += "			      end_date \n"
    sql += " 	      FROM    tmp_ft_links_src_grouped l) AS l2 \n"
    sql += "ON l1.target_node_cleaned = l2.target_node_cleaned \n"
    sql += "WHERE  l1.origin_node_cleaned <> l2.origin_node_cleaned; "

    dfl = betl.DataFlow(
        desc='Generate the P2P_where links')

    dfl.customSQL(
        sql=sql,
        dataLayer='STG',
        dataset='tmp_ft_links_generated_P2P_where')

    # TODO: need to sort this out. This is just a placeholder to make the
    # code work
    dfl.addColumns(
        dataset='tmp_ft_links_generated_P2P_where',
        columns={
            'audit_source_system': None,
            'audit_bulk_load_date': None,
            'audit_latest_delta_load_date': None,
            'audit_latest_load_operation': 'BULK'},
        desc='TODO: create empty audit cols')

    dfl.write(
        dataset='tmp_ft_links_generated_P2P_where',
        targetTableName='tmp_ft_links_generated_P2P_where',
        dataLayerID='STG')


def generateLinks_P2P_while(scheudler):

    sql = ''
    sql += "SELECT LEAST(l1.start_date, l2.start_date) AS start_date, \n"
    sql += "       GREATEST(l1.end_date, l2.end_date) AS end_date, \n"
    sql += "       l1.origin_node_cleaned, \n"
    sql += "       l1.origin_node_type, \n"
    sql += "       l2.origin_node_cleaned AS target_node_cleaned, \n"
    sql += "       l2.origin_node_type AS target_node_type, \n"
    sql += "       l1.target_node_cleaned AS commonality_node_cleaned, \n"
    sql += "       l1.target_node_type AS commonality_node_type, \n"
    sql += "	   'P2P' AS link_type, \n"
    sql += "	   substring(l1.relationship from 1 for \n"
    sql += "                 position('_' in l1.relationship)-1) \n"
    sql += "       || '==' || \n"
    sql += "       substring(l2.relationship from 1 for \n"
    sql += "                 position('_' in l2.relationship)-1) \n"
    sql += "       AS relationship \n"
    sql += "FROM   (SELECT origin_node_cleaned, \n"
    sql += "			   origin_node_type, \n"
    sql += "			   target_node_cleaned, \n"
    sql += "			   target_node_type, \n"
    sql += "			   link_type, \n"
    sql += "			   relationship, \n"
    sql += "			   start_date, \n"
    sql += "			   end_date \n"
    sql += " 	   FROM    tmp_ft_links_src_grouped) AS l1 \n"
    sql += "LEFT JOIN (SELECT origin_node_cleaned, \n"
    sql += "			      origin_node_type, \n"
    sql += "			      target_node_cleaned, \n"
    sql += "			      target_node_type, \n"
    sql += "			      link_type, \n"
    sql += "			      relationship, \n"
    sql += "			      start_date, \n"
    sql += "			      end_date \n"
    sql += " 	      FROM    tmp_ft_links_src_grouped l) AS l2 \n"
    sql += "ON l1.target_node_cleaned = l2.target_node_cleaned \n"
    sql += "AND (coalesce(to_date(l1.start_date, 'YYYYMMDD'),"
    sql += "              to_date('19000101', 'YYYYMMDD')),"
    sql += "     coalesce(to_date(l1.end_date, 'YYYYMMDD'),CURRENT_DATE)) "
    sql += "    overlaps "
    sql += "    (coalesce(to_date(l2.start_date, 'YYYYMMDD'),"
    sql += "              to_date('19000101', 'YYYYMMDD')),"
    sql += "     coalesce(to_date(l2.end_date, 'YYYYMMDD'),CURRENT_DATE))\n"
    sql += "WHERE  l1.origin_node_cleaned <> l2.origin_node_cleaned; "

    dfl = betl.DataFlow(
        desc='Generate the P2P_while links')

    dfl.customSQL(
        sql=sql,
        dataLayer='STG',
        dataset='tmp_ft_links_generated_P2P_while')

    # TODO: need to sort this out. This is just a placeholder to make the
    # code work
    dfl.addColumns(
        dataset='tmp_ft_links_generated_P2P_while',
        columns={
            'audit_source_system': None,
            'audit_bulk_load_date': None,
            'audit_latest_delta_load_date': None,
            'audit_latest_load_operation': 'BULK'},
        desc='TODO: create empty audit cols')

    dfl.write(
        dataset='tmp_ft_links_generated_P2P_while',
        targetTableName='tmp_ft_links_generated_P2P_while',
        dataLayerID='STG')


def prepareFTLinks(scheduler):

    dfl = betl.DataFlow(
        desc='Bring all our different link types together into a single ' +
             'dataset')

    dfl.read(tableName='tmp_ft_links_generated_C2P', dataLayer='STG')
    dfl.read(tableName='tmp_ft_links_generated_P2P_where', dataLayer='STG')
    dfl.read(tableName='tmp_ft_links_generated_P2P_while', dataLayer='STG')

    dfl.union(
        datasets=[
            'tmp_ft_links_generated_C2P',
            'tmp_ft_links_generated_P2P_where',
            'tmp_ft_links_generated_P2P_while'],
        targetDataset='trg_ft_links',
        desc='Union the three datasets')

    dfl.addColumns(
        dataset='trg_ft_links',
        columns={
            'nk_origin_node': setNKCols_originNode,
            'nk_target_node': setNKCols_targetNode,
            'nk_commonality_node': setNKCols_commonalityNode},
        desc='Set the NK nodes')

    dfl.renameColumns(
        dataset='trg_ft_links',
        columns={
            'link_type': 'nk_link_type',
            'relationship': 'nk_relationship',
            'start_date': 'nk_start_date',
            'end_date': 'nk_end_date',
            },
        desc='Rename the other NK columns to nk_')

    dfl.dropColumns(
        dataset='trg_ft_links',
        colsToDrop=[
            'origin_node_cleaned',
            'origin_node_type',
            'target_node_cleaned',
            'target_node_type',
            'commonality_node_cleaned',
            'commonality_node_type'],
        desc='Drop unneeded cols')

    dfl.addColumns(
        dataset='trg_ft_links',
        columns={'dd_duration': calculateDuration},
        desc='Add a duration degenerate dimension')

    dfl.write(
        dataset='trg_ft_links',
        targetTableName='trg_ft_links',
        dataLayerID='STG')


#####################
# UTILITY FUNCTIONS #
#####################

def setLinkType(row):
    if row['link_type'] == 'P2C':
        return 'C2P'
    elif row['link_type'] == 'C2C':
        return 'C2C'


def setRelationship(colToClean):
    cleanCol = colToClean.str.replace('of', 'by')
    return cleanCol


def setNKCols_originNode(row):
    return row['origin_node_type'] + '_' + row['origin_node_cleaned']


def setNKCols_targetNode(row):
    return row['target_node_type'] + '_' + row['target_node_cleaned']


def setNKCols_commonalityNode(row):
    return row['commonality_node_type'] + '_' + row['commonality_node_cleaned']


def calculateDuration(row):
    if row['nk_start_date'] is None or row['nk_start_date'] == '':
        duration = 0
    else:
        startDate = \
            datetime.datetime.strptime(row['nk_start_date'], '%Y%M%d').date()
        if row['nk_end_date'] is None or row['nk_end_date'] == '':
            endDate = datetime.date.today()
        else:
            endDate = \
                datetime.datetime.strptime(row['nk_end_date'], '%Y%M%d').date()
        duration = (endDate - startDate).days
    duration_int = int(duration)

    return duration_int
