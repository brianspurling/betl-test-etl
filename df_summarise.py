import betl


# Not being used, think the loop option below is faster
def buildFtMentions_allInOne(scheduler):

    if scheduler.bulkOrDelta == 'BULK':
        sql = ''
        sql += "SELECT node_id as fk_node, \n"
        sql += "cd.corruption_doc_id as fk_corruption_doc, \n"
        sql += "1 as is_mentioned_count \n"
        sql += "FROM   DM_NODE n \n"
        sql += "INNER JOIN dm_corruption_doc cd \n"
        sql += "ON cd.corruption_doc_content ILIKE '% ' || n.name || ' %' \n"
        sql += "WHERE cd.corruption_doc_id >= 0 \n"
        sql += "AND   n.node_id >= 0 \n"

        df = betl.customSql(sql=sql,
                            dataLayerID='TRG')

        betl.writeData(df, 'su_mentions', 'SUM', 'append', forceDBWrite=True)

    elif scheduler.bulkOrDelta == 'DELTA':
        # TODO:
        pass


def buildFtMentions(scheduler):

    if scheduler.bulkOrDelta == 'BULK':
        df_cd = betl.readData('dm_corruption_doc', 'TRG', forceDBRead='TRG')
        for index, row in df_cd.iterrows():
            cd_id = row['corruption_doc_id']

            sql = ''
            sql += "SELECT node_id as fk_node, \n"
            sql += "cd.corruption_doc_id as fk_corruption_doc, \n"
            sql += "1 as is_mentioned_count \n"
            sql += "FROM   DM_NODE n \n"
            sql += "INNER JOIN dm_corruption_doc cd \n"
            sql += "ON    cd.corruption_doc_content ILIKE " + \
                   "'% ' || n.name || ' %' \n"
            sql += "AND   cd.corruption_doc_id = " + str(cd_id) + "\n"
            sql += "WHERE cd.corruption_doc_id >= 0 \n"
            sql += "AND   n.node_id >= 0 \n"

            df = betl.customSql(sql=sql,
                                dataLayerID='TRG')
            if len(df) > 0:
                betl.writeData(df, 'su_mentions', 'SUM', 'append',
                               forceDBWrite=True)

    elif scheduler.bulkOrDelta == 'DELTA':
        # TODO:
        pass


def writeBackMentions(scheduler):

    if scheduler.bulkOrDelta == 'BULK':
        sql = ''
        sql += "update dm_node n \n"
        sql += "set    mentions_count = \n"
        sql += "           (select sum(m.is_mentioned_count) \n"
        sql += "		   	from   su_mentions m \n"
        sql += "			where  m.fk_node = n.node_id) \n"

        # TODO: custom sql shoudl take the DB, not the DL - which means DB
        # needs to be stored in the schema object too, perhaps?
        betl.customSql(sql=sql, dataLayerID='TRG')

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

        betl.customSql(sql=sql, dataLayerID='TRG')

    elif scheduler.bulkOrDelta == 'DELTA':
        # TODO:
        pass
