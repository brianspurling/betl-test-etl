import betl


def createTextSearchColumns(scheduler):

    if scheduler.bulkOrDelta == 'BULK':

        # pandas and csv does not support tsvector type, so
        # do this in custom sql

        sql = ''
        sql += "UPDATE dm_corruption_doc \n"
        sql += "SET    corruption_doc_tsvector = " + \
               "to_tsvector(corruption_doc_content_alphanumeric)"

        betl.customSql(sql=sql,
                       dataLayerID='TRG')

        sql = ''
        sql += "UPDATE dm_node \n"
        sql += "SET    name_tsquery = " + \
               "plainto_tsquery(name_alphanumeric)"

        betl.customSql(sql=sql,
                       dataLayerID='TRG')

    elif scheduler.bulkOrDelta == 'DELTA':
        pass
