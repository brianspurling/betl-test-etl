import betl


def createTextSearchColumns(scheduler):

    if scheduler.bulkOrDelta == 'BULK':

        sql = ''
        sql += "UPDATE dm_corruption_doc \n"
        sql += "SET    corruption_doc_tsvector = " + \
               "to_tsvector(corruption_doc_content_alphanumeric)"

        dfl = betl.DataFlow(
            desc='Create the search columns in the dimensions (' +
                 'pandas and csv does not support tsvector type, so ' +
                 'do this in custom sql)')

        dfl.customSQL(
            sql=sql,
            dataLayer='TRG',
            desc='Setting corruption_doc_tsvector on dm_corruption_doc')

        sql = ''
        sql += "UPDATE dm_node \n"
        sql += "SET    name_tsquery = " + \
               "plainto_tsquery(name_alphanumeric)"

        dfl.customSQL(
            sql=sql,
            dataLayer='TRG',
            desc='Setting name_tsquery on dm_node')

        dfl.close()

    elif scheduler.bulkOrDelta == 'DELTA':
        pass
