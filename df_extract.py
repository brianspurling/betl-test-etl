import betl

import pandas as pd
from datetime import datetime


#
#
#
def extractPosts():
    funcName = 'extractPosts'
    logStr = ''

    srcLayerSchema = betl.getSrcLayerSchema()
    bulkOrDelta = betl.isBulkOrDelta()
    conn = srcLayerSchema.srcSystemConns['WP']

    if bulkOrDelta == 'BULK':

        a = 'Full extract'
        tableName = 'documents'
        fullFilePath = 'data/' + tableName + '.csv'
        df = pd.read_csv(filepath_or_buffer=fullFilePath,
                         sep=conn.files[tableName]['delimiter'],
                         quotechar=conn.files[tableName]['quotechar'])

        betl.setAuditCols(df, 'WP', 'BULK')

        logStr += betl.describeDF(funcName, a, df, 1)

        #
        # Write to SRC
        #
        time = str(datetime.time(datetime.now()))
        # Bulk load the SRC table
        a = 'Bulk writing ' + tableName + ' to SRC (start: ' + time + ')'
        logStr += betl.describeDF(funcName, a, df, 2)

        # if_exists='replace' covers the truncate for us
        df.to_sql('src_wp_' + tableName,
                  betl.getEtlDBEng(),
                  if_exists='replace',
                  index=False)
        a = tableName + ' written to SRC (end: ' + time + ')'
        logStr += betl.describeDF(funcName, a, df, 2)

        return logStr

    if bulkOrDelta == 'DELTA':
        # TODO: #48 (question for AJ)
        pass
