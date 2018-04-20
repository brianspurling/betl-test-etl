import betl


def extractPosts(scheduler):

    if scheduler.bulkOrDelta == 'BULK':

        # TODO, was I not planning on using the ID here, and determingin
        # the file name from the schema, or something, like dbs, or is that
        # only for default extract? which of course I'm not doing for any files
        # in this app. i have the extension saved in appConfig...
        df = betl.readDataFromSrcSys(srcSysID='WP',
                                     file_name_or_table_name='documents')

        betl.logStepStart('Setting audit columns', 1)
        df = betl.setAuditCols(df, 'WP', 'BULK')
        betl.logStepEnd(df)

        betl.writeData(df=df,
                       tableName='src_wp_documents',
                       dataLayerID='SRC',
                       forceDBWrite=True)

    elif scheduler.bulkOrDelta == 'DELTA':
        # TODO: #48 (question for AJ)
        pass
