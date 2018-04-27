import betl
import datetime


def loadSrcLinksIntoTempFile(scheduler):
    df = betl.readData('mrg_src_links', 'STG')
    df['commonality_node_cleaned'] = None
    df['commonality_node_type'] = None
    betl.writeData(df, 'tmp_ft_links', 'STG')


def generateLinks_C2P(scheduler):

    df = betl.readData('mrg_src_links', 'STG')
    cols = list(df)

    betl.logStepStart('Rename cols to swap origin/target over and reorder', 1)
    df.rename(index=str,
              columns={
                'origin_node_cleaned': 'target_node_cleaned',
                'origin_node_type': 'target_node_type',
                'target_node_cleaned': 'origin_node_cleaned',
                'target_node_type': 'origin_node_type'},
              inplace=True)
    df = df[cols]
    betl.logStepEnd(df)

    betl.logStepStart('Set values of link_type and relationship', 2)
    df['link_type'] = df.apply(setLinkType, axis=1)
    df['relationship'] = df.apply(
        lambda row: row.relationship.replace('of', 'by'), axis=1)
    betl.logStepEnd(df)

    betl.writeData(df, 'tmp_ft_links', 'STG', 'append')


def setLinkType(row):
    if row['link_type'] == 'P2C':
        return 'C2P'
    elif row['link_type'] == 'C2C':
        return 'C2C'


def generateLinks_P2P_prep(scheduler):
    df = betl.readData('mrg_src_links', 'STG')

    betl.logStepStart('Add link type)', 1)
    df = df.loc[df['link_type'] == 'P2C']
    betl.logStepEnd(df)

    # Force this to DB because next step is SQL that we must push down to DB
    betl.writeData(df, 'tmp_ft_links_src_grouped', 'STG', forceDBWrite=True)


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

    df = betl.customSql(sql, 'STG')

    betl.logStepStart('Reorder columns', 1)
    cols = betl.getColumnHeadings('tmp_ft_links', 'STG')
    df = df[cols]
    betl.logStepEnd(df)

    betl.writeData(df, 'tmp_ft_links', 'STG', 'append')


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

    df = betl.customSql(sql, 'STG')

    cols = betl.getColumnHeadings('tmp_ft_links', 'STG')

    betl.logStepStart('Reorder columns', 1)
    df = df[cols]
    betl.logStepEnd(df)

    betl.writeData(df, 'tmp_ft_links', 'STG', 'append')


def prepareFTLinks(scheduler):
    df = betl.readData('tmp_ft_links', 'STG')

    # betl.logStepStart('Filter to relationship: sh==sh', 0)
    # df = df.loc[df['relationship'] == 'sh==sh']
    # betl.logStepEnd(df)

    betl.logStepStart('Create NK columns', 1)
    df['nk_origin_node'] = (df['origin_node_type'] +
                            '_' + df['origin_node_cleaned'])
    df['nk_target_node'] = (df['target_node_type'] +
                            '_' + df['target_node_cleaned'])
    df['nk_commonality_node'] = (df['commonality_node_type'] +
                                 '_' + df['commonality_node_cleaned'])
    df.rename(index=str,
              columns={
                  'link_type': 'nk_link_type',
                  'relationship': 'nk_relationship',
                  'start_date': 'nk_start_date',
                  'end_date': 'nk_end_date'},
              inplace=True)

    betl.logStepEnd(df)

    betl.logStepStart('Drop unneeded columns', 3)
    df.drop([
        'origin_node_cleaned',
        'origin_node_type',
        'target_node_cleaned',
        'target_node_type',
        'commonality_node_cleaned',
        'commonality_node_type'],
        axis=1,
        inplace=True)
    betl.logStepEnd(df)

    betl.logStepStart('Add duration DD', 4)
    df['dd_duration'] = 0
    df['dd_duration'] = df.apply(calculateDuration, axis=1)
    betl.logStepEnd(df)

    betl.writeData(df, 'trg_ft_links', 'STG')


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
