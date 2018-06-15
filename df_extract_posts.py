def extractPosts(betl):

    if betl.CONF.EXE.BULK_OR_DELTA == 'BULK':

        dfl = betl.DataFlow(desc='Extract WordPress posts from source')

        dfl.getDataFromSrc(
            tableName='documents',
            srcSysID='WP')

        dfl.setAuditCols(
            dataset='documents',
            bulkOrDelta='BULK',
            sourceSystem='WP')

        dfl.write(dataset='documents',
                  targetTableName='src_wp_documents',
                  dataLayerID='SRC')

    elif betl.CONF.EXE.BULK_OR_DELTA == 'DELTA':
        pass
