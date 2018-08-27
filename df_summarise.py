import pandas as pd

ALL_CONTENT_CLEAN = None
ALL_CONTENT_ALPHA = None
DF_MENTIONED_NODES = None
DF_SU_MENTIONS = None
DF_CORRUPTION_DOCS = None


def buildSuMentions(betl):

    global ALL_CONTENT_CLEAN
    global ALL_CONTENT_ALPHA
    global DF_MENTIONED_NODES
    global DF_SU_MENTIONS
    global DF_CORRUPTION_DOCS

    if betl.CONF.EXE.BULK_OR_DELTA == 'BULK':

        dfl = betl.DataFlow(
            desc='Build su_mentions by searching all docs for all node names')

        # FIRST ITERATION

        dfl.read(
            tableName='dm_corruption_doc',
            dataLayer='BSE',
            forceDBRead=True)

        dfl.read(
            tableName='dm_node',
            dataLayer='BSE',
            forceDBRead=True)

        cols = dfl.getColumns(
            dataset='dm_corruption_doc',
            columnNames=[
                'corruption_doc_content_cleaned',
                'corruption_doc_content_alphanumeric'],
            desc='Creating two large strings of all doc content ' +
                 '(one for the cleaned text and one for the alphanumeric)')

        ALL_CONTENT_CLEAN = \
            cols['corruption_doc_content_cleaned'].str.cat(sep=' . ')
        ALL_CONTENT_ALPHA = \
            cols['corruption_doc_content_alphanumeric'].str.cat(sep=' . ')
        DF_MENTIONED_NODES = pd.DataFrame(
            columns=['node_id', 'name', 'name_alphanumeric'])

        dfl.applyFunctionToRows(
            dataset='dm_node',
            function=searchAllContentForNodeName,
            desc='Identifing all nodes mentioned somewhere. This is the ' +
                 'big one! Takes about 5 hours, and searches all documents ' +
                 '(in a single string) for every node name (one by one). ' +
                 'It tries the cleaned doc content first, then tries the ' +
                 'alphanumeric. If either succeeds we have a match')

        dfl.createDataset(
            dataset='mentioned_nodes',
            data=DF_MENTIONED_NODES,
            desc='Loaded our generated mentioned_nodes df into the dataflow')

        # SECOND ITERATION

        DF_SU_MENTIONS = pd.DataFrame(
            columns=[
                'fk_node',
                'fk_corruption_doc',
                'mentions_count',
                'is_mentioned_count'])
        # TODO: urgh, global var here, is there a better way?
        DF_CORRUPTION_DOCS = dfl.getDataFrames(datasets='dm_corruption_doc')

        dfl.applyFunctionToRows(
            dataset='mentioned_nodes',
            function=searchDMCorruptionDocsForMentionedNodeNames,
            desc='Using our new list of mentioned nodes, now create ' +
                 'su_mentions properly (i.e. doc/node pairs) by ' +
                 'iterating through our mentioned nodes and searching ' +
                 'dm_corruption_docs (cleaned and alpha) for each')

        dfl.createDataset(
            dataset='su_mentions',
            data=DF_SU_MENTIONS,
            desc='Loaded our generated su_mentions df into the dataflow')

        dfl.filter(
            dataset='su_mentions',
            filters={'mentions_count': ('>', 0)},
            desc='Filter to mentioned nodes only (because we found the ' +
                 'node/doc pairs by constantly generating a count for ' +
                 'every node in every doc)')

        dfl.write(
            dataset='su_mentions',
            targetTableName='su_mentions',
            dataLayerID='SUM',
            forceDBWrite=True,
            append_or_replace='append',  # stops it removing SK!
            desc='Writing to summary DB layer')

    elif betl.CONF.EXE.BULK_OR_DELTA == 'DELTA':
        pass


def writeBackMentions(betl):

    if betl.CONF.EXE.BULK_OR_DELTA == 'BULK':
        dfl = betl.DataFlow(
            desc='Write-back the mentions counts to dm_node and ' +
                 'dm_corruption_doc')

        sql = ''
        sql += "update dm_node n \n"
        sql += "set    mentions_count = \n"
        sql += "           (select sum(m.is_mentioned_count) \n"
        sql += "		   	from   su_mentions m \n"
        sql += "			where  m.fk_node = n.node_id) \n"

        dfl.customSQL(
            sql=sql,
            dataLayer='BSE',
            desc='Setting mentions_count on dm_node')

        sql = ''
        sql += "update dm_corruption_doc cd \n"
        sql += "set    number_mentioned_nodes = \n"
        sql += "           (select sum(m.is_mentioned_count) \n"
        sql += "		   	from   su_mentions m \n"
        sql += "			where  1=1 \n"
        sql += "            and m.fk_corruption_doc = cd.corruption_doc_id),\n"
        sql += "       number_mentioned_people = \n"
        sql += "           (select sum(m.is_mentioned_count) \n"
        sql += "		   	from   su_mentions m \n"
        sql += "		   	inner join dm_node n \n"
        sql += "		   	on     m.fk_node = n.node_id \n"
        sql += "			where  n.node_type in ('person') \n"
        sql += "            and m.fk_corruption_doc = cd.corruption_doc_id),\n"
        sql += "       number_mentioned_companies = \n"
        sql += "           (select sum(m.is_mentioned_count) \n"
        sql += "		   	from   su_mentions m \n"
        sql += "		   	inner join dm_node n \n"
        sql += "		   	on     m.fk_node = n.node_id \n"
        sql += "			where  n.node_type in ('company') \n"
        sql += "            and m.fk_corruption_doc = cd.corruption_doc_id) \n"

        dfl.customSQL(
            sql=sql,
            dataLayer='BSE',
            desc='Setting number_mentioned_nodes / _people / _companies ' +
                 ' on dm_corruption_doc')

        dfl.close()

    elif betl.CONF.EXE.BULK_OR_DELTA == 'DELTA':
        pass


#####################
# UTILITY FUNCTIONS #
#####################


def searchAllContentForNodeName(row):

    global DF_MENTIONED_NODES

    if(row[3] in ALL_CONTENT_CLEAN):
        DF_MENTIONED_NODES.loc[len(DF_MENTIONED_NODES)] = \
            [row[1], row[3], row[4]]
    elif(row[4] in ALL_CONTENT_ALPHA):
        DF_MENTIONED_NODES.loc[len(DF_MENTIONED_NODES)] = \
            [row[1], row[3], row[4]]


def searchDMCorruptionDocsForMentionedNodeNames(row):

    global DF_SU_MENTIONS

    # This is the main text search
    matches_clean = \
        DF_CORRUPTION_DOCS['corruption_doc_content_cleaned'] \
        .str.count(row[2])
    matches_alpha = \
        DF_CORRUPTION_DOCS['corruption_doc_content_alphanumeric'] \
        .str.count(row[3])
    df_matches_clean = pd.DataFrame(matches_clean)
    df_matches_alpha = pd.DataFrame(matches_alpha)
    df_matches_clean.columns = ['mentions_count_clean']
    df_matches_alpha.columns = ['mentions_count_alpha']

    df_temp = pd.DataFrame(
        columns=[
            'fk_corruption_doc',
            'fk_node',
            'mentions_count',
            'mentions_count_clean',
            'mentions_count_alpha',
            'is_mentioned_count'])
    df_temp['fk_corruption_doc'] = \
        pd.to_numeric(DF_CORRUPTION_DOCS['corruption_doc_id'])
    nodeID = int(row[1])
    df_temp['fk_node'] = nodeID
    df_temp['mentions_count_clean'] = \
        pd.to_numeric(
            df_matches_clean['mentions_count_clean'],
            downcast='integer')
    df_temp['mentions_count_alpha'] = \
        pd.to_numeric(
            df_matches_alpha['mentions_count_alpha'],
            downcast='integer')
    df_temp['mentions_count'] = \
        df_temp['mentions_count_clean'] + \
        df_temp['mentions_count_alpha']
    df_temp.drop(
        ['mentions_count_clean', 'mentions_count_alpha'],
        axis=1,
        inplace=True)
    DF_SU_MENTIONS = pd.concat(objs=[DF_SU_MENTIONS, df_temp])
