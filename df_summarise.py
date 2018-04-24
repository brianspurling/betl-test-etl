import betl
import pandas as pd


def buildFtMentions(scheduler):
    if scheduler.bulkOrDelta == 'BULK':

        df_cd = betl.readData('dm_corruption_doc', 'TRG', forceDBRead='TRG')
        df_n = betl.readData('dm_node', 'TRG', forceDBRead='TRG')
        df_n['name'] = df_n['name'].str.lower()

        betl.logStepStart('Creating a single object for all documents')
        allContent = df_cd['corruption_doc_content'].str.cat(sep='. ').lower()
        betl.logStepEnd()

        betl.logStepStart('Identifing all nodes mentioned somewhere')
        # this is the big one. Takes about 3 hours, and searches all documents
        # (in a single string) for every node name (one by one)
        df_mn = pd.DataFrame(columns=['node_id', 'name'])
        for row in df_n.itertuples():
            if(row[3] in allContent):
                df_mn.loc[len(df_mn)] = [row[1], row[3]]
        betl.logStepEnd(df_mn)

        betl.logStepStart('Creating su_mentions (doc/node pairs)')
        df_m = pd.DataFrame(columns=[
            'fk_node',
            'fk_corruption_doc',
            'mentions_count',
            'is_mentioned_count'])

        for node in df_mn.itertuples():

            # This is the main text search
            matches = df_cd['corruption_doc_content'].str.count(node[2])
            df_matches = pd.DataFrame(matches)
            df_matches.columns = ['mentions_count']

            df_temp = pd.DataFrame(
                columns=[
                    'fk_corruption_doc',
                    'fk_node',
                    'mentions_count',
                    'is_mentioned_count'])
            df_temp['fk_corruption_doc'] = \
                pd.to_numeric(df_cd['corruption_doc_id'])
            nodeID = int(node[1])
            df_temp['fk_node'] = nodeID
            df_temp['mentions_count'] = \
                pd.to_numeric(df_matches['mentions_count'], downcast='integer')
            df_m = pd.concat(objs=[df_m, df_temp])

        df_m = df_m.loc[df_m['mentions_count'] > 0]
        betl.logStepEnd(df_m)

        betl.writeData(df_m, 'su_mentions', 'SUM', forceDBWrite=True)

    elif scheduler.bulkOrDelta == 'DELTA':
        pass


def writeBackMentions(scheduler):

    if scheduler.bulkOrDelta == 'BULK':
        sql = ''
        sql += "update dm_node n \n"
        sql += "set    mentions_count = \n"
        sql += "           (select sum(m.is_mentioned_count) \n"
        sql += "		   	from   su_mentions m \n"
        sql += "			where  m.fk_node = n.node_id) \n"

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
        pass
