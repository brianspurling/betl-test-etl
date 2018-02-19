import betl


#
#
#
def extractPosts():
    srcLayerSchema = betl.getSrcLayerSchema()
    bulkOrDelta = betl.isBulkOrDelta()
    conn = srcLayerSchema.srcSystemConns['WP']

    if bulkOrDelta == 'BULK':

        tableName = 'documents'
        fullFilePath = 'data/' + tableName + '.csv'
        df = betl.readFromCsv(file_or_filename=fullFilePath,
                              sep=conn.files[tableName]['delimiter'],
                              quotechar=conn.files[tableName]['quotechar'],
                              pathOverride=True)

        betl.logStepStart('Setting audit columns', 1)
        betl.setAuditCols(df, 'WP', 'BULK')
        betl.logStepEnd(df)

        betl.writeToCsv(df, 'src_wp_' + tableName)

    if bulkOrDelta == 'DELTA':
        # TODO: #48 (question for AJ)
        pass
