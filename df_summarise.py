import betl
import pandas as pd


def buildFtMentions(scheduler):
    if scheduler.bulkOrDelta == 'BULK':

        df_cd = betl.readData('dm_corruption_doc', 'TRG', forceDBRead='TRG')
        df_n = betl.readData('dm_node', 'TRG', forceDBRead='TRG')

        betl.logStepStart('Creating two large strings of all doc content ' +
                          '(clean and alphanumeric)')
        allContent_clean = \
            df_cd['corruption_doc_content_cleaned'].str.cat(sep=' . ')
        allContent_alpha = \
            df_cd['corruption_doc_content_alphanumeric'].str.cat(sep=' . ')
        betl.logStepEnd()

        # this is the big one. Takes about 5 hours, and searches all documents
        # (in a single string) for every node name (one by one)
        betl.logStepStart('Identifing all nodes mentioned somewhere')
        df_mn = pd.DataFrame(columns=[
            'node_id',
            'name',
            'name_alphanumeric'])
        count = 0
        for row in df_n.itertuples():
            count += 1
            if(row[3] in allContent_clean):
                df_mn.loc[len(df_mn)] = [row[1], row[3], row[4]]
            elif(row[4] in allContent_alpha):
                df_mn.loc[len(df_mn)] = [row[1], row[3], row[4]]
            # Approx once ever x minutes, write to file 
            if count % 10000 == 0:
                betl.writeData(
                    df_mn,
                    'tmp_su_mentions',
                    'SUM')
        betl.logStepEnd(df_mn)

        betl.writeData(df_mn, 'tmp_su_mentions', 'SUM')

        betl.logStepStart('Creating su_mentions (doc/node pairs)')
        df_m = pd.DataFrame(columns=[
            'fk_node',
            'fk_corruption_doc',
            'mentions_count',
            'is_mentioned_count'])

        for node in df_mn.itertuples():

            # This is the main text search
            matches_clean = \
                df_cd['corruption_doc_content_cleaned'].str.count(node[3])
            matches_alpha = \
                df_cd['corruption_doc_content_alphanumeric'].str.count(node[4])
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
                pd.to_numeric(df_cd['corruption_doc_id'])
            nodeID = int(node[1])
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
