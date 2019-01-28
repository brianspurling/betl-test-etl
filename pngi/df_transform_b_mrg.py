def loadCompaniesToMRG(betl):

    dfl = betl.DataFlow(
        desc='Create a unique list of cleaned company names - we consider ' +
             'two identical names to be the same company')

    dfl.read(tableName='ods_companies', dataLayer='TRN')

    dfl.dropColumns(
        dataset='ods_companies',
        colsToKeep=['company_name_cleaned'],
        desc='Drop all cols other than company_name_cleaned')

    dfl.dedupe(dataset='ods_companies',
               desc='Make list of cleaned company names unique')

    dfl.write(
        dataset='ods_companies',
        targetTableName='mrg_companies',
        dataLayerID='TRN')


def loadPeopleToMRG(betl):

    dfl = betl.DataFlow(
        desc='Create a unique list of cleaned person names - we consider ' +
             'two identical names to be the same person')

    dfl.read(tableName='ods_people', dataLayer='TRN')

    dfl.dropColumns(
        dataset='ods_people',
        colsToKeep=['person_name_cleaned'],
        desc='Drop all cols other than person_name_cleaned')

    dfl.dedupe(dataset='ods_people',
               desc='Make list of cleaned person names unique')

    dfl.write(
        dataset='ods_people',
        targetTableName='mrg_people',
        dataLayerID='TRN')


def loadAddressesToMRG(betl):

    dfl = betl.DataFlow(
        desc='Create a unique list of cleaned addresses - we consider ' +
             'two identical addresses to be the same address')

    dfl.read(tableName='ods_addresses', dataLayer='TRN')

    dfl.dropColumns(
        dataset='ods_addresses',
        colsToKeep=['address_cleaned'],
        desc='Drop all cols other than address_cleaned')

    dfl.dedupe(dataset='ods_addresses',
               desc='Make list of cleaned addresses unique')

    dfl.write(
        dataset='ods_addresses',
        targetTableName='mrg_addresses',
        dataLayerID='TRN')


# In theory, there shouldn't be any duplicate links, because this is the
# lowest grain on the IPA website. I.e. you enter a "link" ("I was director of
# this company between these dates"), and that's where we get people/company
# dimensional data from. Hence why people/companies can be duplicated. But
# for a link to be duplicated, I guess somebody must have entered the same
# data twice...?
# We're deduping here anyway as a precaution, as we move from original to
# cleaned grain. Because if there are two people with the same name, who have
# the same role at the same company for the same dates, we now consider that
# to be a genuine DQ issue (by virtue of the fact that we consdier them to be
# the same person), and hence we can solve that DQ issue (duplicated edge in
# network graph) by deduping here.

def loadSrcLinksToMRG(betl):

    dfl = betl.DataFlow(
        desc='Create a unique list of src_links (should already be unique)')

    dfl.read(tableName='ods_src_links', dataLayer='TRN')

    dfl.dropColumns(
        dataset='ods_src_links',
        colsToDrop=['origin_node_original', 'target_node_original'],
        desc='Drop the origin/target original node names')

    dfl.dedupe(dataset='ods_src_links',
               desc='Make src_links unique (should already be unique)')

    dfl.write(
        dataset='ods_src_links',
        targetTableName='mrg_src_links',
        dataLayerID='TRN')
