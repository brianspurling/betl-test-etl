import betl


def extractPosts(scheduler):

    if scheduler.bulkOrDelta == 'BULK':

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

    elif scheduler.bulkOrDelta == 'DELTA':
        pass
