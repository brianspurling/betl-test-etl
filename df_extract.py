import betl


def extractPosts(scheduler):

    if scheduler.bulkOrDelta == 'BULK':

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
        pass
