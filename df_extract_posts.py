def extractPosts(betl):

    if betl.CONF.EXE.BULK_OR_DELTA == 'BULK':

        dfl = betl.DataFlow(desc='Extract WordPress posts from source')

        dfl.getDataFromSrc(
            tableName='documents',
            srcSysID='WP',
            desc="Read data from WP.documents source")

        dfl.setAuditCols(
            dataset='documents',
            bulkOrDelta='BULK',
            sourceSystem='WP',
            desc="Set the audit columns on WP.documents")

        dfl.write(dataset='documents',
                  targetTableName='src_wp_documents',
                  dataLayerID='SRC',
                  desc="Write documents data to SRC data layer")

    elif betl.CONF.EXE.BULK_OR_DELTA == 'DELTA':
        pass
