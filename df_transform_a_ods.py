import betl
import pandas as pd


#########################
# DATA PROCESSING FUNCS #
#########################

def loadCompaniesToODS(scheduler):

    dfl = betl.DataFlow(
        desc='Extract companies from src_ipa_companies, and shareholder ' +
             'companies from src_ipa_shareholders. Clean them up, union ' +
             'them together, and output to ods_companies')

    dfl.read(tableName='src_ipa_companies', dataLayer='SRC')

    dfl.dropColumns(
        dataset='src_ipa_companies',
        colsToKeep=['company_number', 'company_name'],
        desc='Drop all cols except name and number')

    dfl.renameColumns(
        dataset='src_ipa_companies',
        columns={'company_name': 'company_name_original'},
        desc='Rename company_name to company_name_original')

    dfl.addColumns(
        dataset='src_ipa_companies',
        columns={
            'appointment_date': None,
            'ceased': None,
            'company_shares_held_name_original': None,
            'company_shares_held_name_cleaned': None,
            'company_shares_held_number': None,
            'is_shareholder': 'No'},
        desc='Add additional (blank) cols to match sh rows')

    dfl.read('src_ipa_shareholders', 'SRC')

    dfl.filter(
        dataset='src_ipa_shareholders',
        filters={'is_company': '1'},
        desc='Filter company shareholders')

    dfl.dropColumns(
        dataset='src_ipa_shareholders',
        colsToKeep=[
            'name',
            'shareholding_company_number',
            'appointed',
            'ceased',
            'company_name',
            'company_number'],
        desc='Drop cols to make shareholder dataset compatible')

    dfl.addColumns(
        dataset='src_ipa_shareholders',
        columns={'is_shareholder': 'YES',
                 'company_shares_held_name_cleaned': None},
        desc='Add is_shareholder and name_cleaned cols')

    dfl.renameColumns(
        dataset='src_ipa_shareholders',
        columns={'name': 'company_name_original',
                 'shareholding_company_number': 'company_number',
                 'company_name': 'company_shares_held_name_original',
                 'company_number': 'company_shares_held_number',
                 'appointed': 'appointment_date'},
        desc='Rename cols')

    dfl.cleanColumn(
        dataset='src_ipa_shareholders',
        cleaningFunc=cleanCompanyName,
        column='company_shares_held_name_original',
        cleanedColumn='company_shares_held_name_cleaned',
        desc='Add a clean company_name column for the shares_held_in company')

    dfl.cleanColumn(
        dataset='src_ipa_shareholders',
        cleaningFunc=validateStringDates,
        column='appointment_date',
        desc='Convert appointment_date to Date and back to Str (YYYYMMDD),' +
             ' remove NaNs')

    dfl.cleanColumn(
        dataset='src_ipa_shareholders',
        cleaningFunc=validateStringDates,
        column='ceased',
        desc='Convert ceased date to Date and back to Str (YYYYMMDD),' +
             ' remove NaNs')

    dfl.union(
        datasets=['src_ipa_companies', 'src_ipa_shareholders'],
        targetDataset='ods_companies',
        desc='Union the two datasets')

    dfl.cleanColumn(
        dataset='ods_companies',
        cleaningFunc=cleanCompanyName,
        column='company_name_original',
        cleanedColumn='company_name_cleaned',
        desc="Add a cleaned company_name column for the node's company name")

    dfl.write(
        dataset='ods_companies',
        targetTableName='ods_companies',
        dataLayerID='STG')


def loadPeopleToODS(scheduler):

    dfl = betl.DataFlow(
        desc='Extract people from src_ipa_directors, _shareholders & ' +
             '_secretaries, combine into a single dataset and clean up a bit')

    trgCols = [
        'name',
        'company_name',
        'appointment_date',
        'ceased',
        'residential_address',
        'postal_address',
        'nationality',
        'role_type']

    # DIRECTORS

    dfl.read(tableName='src_ipa_directors', dataLayer='SRC')

    dfl.addColumns(
        dataset='src_ipa_directors',
        columns={'role_type': 'DIRECTOR'},
        desc='Add column indicating these are directors')

    dfl.dropColumns(
        dataset='src_ipa_directors',
        colsToKeep=trgCols,
        desc='Drop cols to make directors dataset compatible')

    # SHAREHOLDERS

    dfl.read(tableName='src_ipa_shareholders', dataLayer='SRC')

    dfl.filter(
        dataset='src_ipa_shareholders',
        filters={'is_company': '0'},
        desc='Filter to human shareholders')

    dfl.addColumns(
        dataset='src_ipa_shareholders',
        columns={'role_type': 'SHAREHOLDER'},
        desc='Add column indicating these are shareholders')

    dfl.dropColumns(
        dataset='src_ipa_shareholders',
        colsToDrop=['appointment_date'],
        desc='Drop the appointment_date column from sh (we will use the ' +
             'appointed col instead)')

    dfl.renameColumns(
        dataset='src_ipa_shareholders',
        columns={'appointed': 'appointment_date'},
        desc='Rename appointed to appointment_date')

    dfl.dropColumns(
        dataset='src_ipa_shareholders',
        colsToKeep=trgCols,
        desc='Drop cols to make shareholders dataset compatible')

    # SECRETARIES

    dfl.read(tableName='src_ipa_secretaries', dataLayer='SRC')

    dfl.addColumns(
        dataset='src_ipa_secretaries',
        columns={'role_type': 'SECRETARY'},
        desc='Add column indicating these are secretaries')

    dfl.dropColumns(
        dataset='src_ipa_secretaries',
        colsToDrop=['appointment_date'],
        desc='Drop the appointment_date column from secs (we will use ' +
             'the appointed col instead)')

    dfl.renameColumns(
        dataset='src_ipa_secretaries',
        columns={'appointed_date': 'appointment_date'},
        desc='Rename appointed_date to appointment_date')

    dfl.dropColumns(
        dataset='src_ipa_secretaries',
        colsToKeep=trgCols,
        desc='Drop cols to make secretaries datasets compatible')

    # ODS_PEOPLE

    dfl.union(
        datasets=[
            'src_ipa_directors',
            'src_ipa_shareholders',
            'src_ipa_secretaries'],
        targetDataset='ods_people',
        desc='Union the three datasets')

    dfl.renameColumns(
        dataset='ods_people',
        columns={'name': 'person_name_original',
                 'company_name': 'company_name_original'},
        desc='Rename name cols to _name_original')

    dfl.cleanColumn(
        dataset='ods_people',
        cleaningFunc=cleanPersonName,
        column='person_name_original',
        cleanedColumn='person_name_cleaned',
        desc='person_name is one or our node NKs, so we need to clean ' +
             'it to ensure we treat different variations of the same name ' +
             'as the same node')

    dfl.cleanColumn(
        dataset='ods_people',
        cleaningFunc=cleanCompanyName,
        column='company_name_original',
        cleanedColumn='company_name_cleaned',
        desc='company_name is one of our node NKs, so we need to clean ' +
             'it to ensure we treat different variations of the same name ' +
             'as the same node')

    dfl.cleanColumn(
        dataset='ods_people',
        cleaningFunc=validateStringDates,
        column='appointment_date',
        desc='Convert appointment_date to Date and back to Str (YYYYMMDD),' +
             ' remove NaNs')

    dfl.cleanColumn(
        dataset='ods_people',
        cleaningFunc=validateStringDates,
        column='ceased',
        desc='Convert ceased to Date and back to Str (YYYYMMDD),' +
             ' remove NaNs')

    dfl.write(
        dataset='ods_people',
        targetTableName='ods_people',
        dataLayerID='STG')


def loadAddressesToODS(scheduler):

    dfl = betl.DataFlow(
        desc='Extract addresses from src_ipa_addresses (these are company ' +
             'addresses), and the directors/shareholders/seretaries tables ' +
             '(these are people addresses)')

    # COMPANY ADDRESSES

    dfl.read(tableName='src_ipa_addresses', dataLayer='SRC')

    # company_name,
    # company_number,
    # address_type,
    # address,
    # start_date,
    # end_date

    dfl.renameColumns(
        dataset='src_ipa_addresses',
        columns={'address': 'address_original'},
        desc='Rename address to address_original')

    dfl.addColumns(
        dataset='src_ipa_addresses',
        columns={'src_table': 'ADDRESSES'},
        desc='Add column to indicate source table for company addresses rows')

    # DIRECTOR'S ADDRSSES

    dfl.read(tableName='src_ipa_directors', dataLayer='SRC')

    # company_name,
    # company_number,
    # appointment_date,
    # name,
    # residential_address,
    # postal_address,
    # nationality,
    # this_person_has_consented_to_act_as_a_director_for_this_company,
    # ceased

    dfl.dropColumns(
        dataset='src_ipa_directors',
        colsToDrop=[
            'name',
            'nationality',
            'this_person_has_consented_to_act_as_a_director_for_this_company'],
        desc='Drop unneeded columns from directors')

    dfl.renameColumns(
        dataset='src_ipa_directors',
        columns={
            'appointment_date': 'start_date',
            'ceased': 'end_date'},
        desc='Rename date columns on directors')

    dfl.duplicateDataset(
        dataset='src_ipa_directors',
        targetDatasets=[
            'director_residential_addresses',
            'director_postal_addresses'],
        desc='Duplicate directors into two datasets, ready to union')

    # RESIDENTIAL DIRECTOR ADDRESSES

    dfl.dropColumns(
        dataset='director_residential_addresses',
        colsToDrop=['postal_address'],
        desc='Drop the postal address from the dir residential table')

    dfl.renameColumns(
        dataset='director_residential_addresses',
        columns={'residential_address': 'address_original'},
        desc='Rename dir residential_address to address_original')

    dfl.addColumns(
        dataset='director_residential_addresses',
        columns={'address_type': 'residential'},
        desc='Add address_type type column: res directors')

    # POSTAL DIRECTOR ADDRESSES

    dfl.dropColumns(
        dataset='director_postal_addresses',
        colsToDrop=['residential_address'],
        desc='Drop the dir residential address from the postal table')

    dfl.renameColumns(
        dataset='director_postal_addresses',
        columns={'postal_address': 'address_original'},
        desc='Rename dir postal_address to address_original')

    dfl.addColumns(
        dataset='director_postal_addresses',
        columns={'address_type': 'postal'},
        desc='Add address_type type column: postal dirs')

    # UNION DIRECTOR ADDRESSES

    dfl.union(
        datasets=[
            'director_residential_addresses',
            'director_postal_addresses'],
        targetDataset='director_addresses',
        desc='Union the two directors address cols into a single dataset')

    dfl.addColumns(
        dataset='director_addresses',
        columns={'src_table': 'DIRECTORS'},
        desc='Add column to indicate source table for director addresses')

    # SHAREHOLDERS

    dfl.read(tableName='src_ipa_shareholders', dataLayer='SRC')

    # company_name,
    # company_number,
    # shareholder_is_also_a_director,
    # residential_or_registered_office_address,
    # postal_address,
    # nationality,
    # this_person_has_consented_to_act_as_a_shareholder_for_this_company,
    # appointed,
    # name,
    # shareholding_company_number,
    # is_company,
    # place_of_incorporation,
    # this_company_has_consented_to_act_as_a_shareholder_for_this_company,
    # this_entity_has_consented_to_act_as_a_shareholder_for_this_company,
    # ceased,
    # company_name_or_number,
    # residential_address,
    # this_person_has_consented_to_act_as_a_director_for_this_company,
    # appointment_date,

    dfl.dropColumns(
        dataset='src_ipa_shareholders',
        colsToKeep=[
            'company_name',
            'company_number',
            'appointed',
            'ceased',
            'residential_address',
            'postal_address'],
        desc='Drop unneeded columns from shareholders')

    dfl.renameColumns(
        dataset='src_ipa_shareholders',
        columns={
            'appointed': 'start_date',
            'ceased': 'end_date'},
        desc='Rename date columns on shareholders')

    dfl.duplicateDataset(
        dataset='src_ipa_shareholders',
        targetDatasets=[
            'shareholders_residential_addresses',
            'shareholders_postal_addresses'],
        desc='Duplicate shareholders into two datasets, ready to union')

    # SHAREHOLDER RESIDENTIAL ADDRESSES

    dfl.dropColumns(
        dataset='shareholders_residential_addresses',
        colsToDrop=['postal_address'],
        desc='Drop the postal address from the sh residential table')

    dfl.renameColumns(
        dataset='shareholders_residential_addresses',
        columns={'residential_address': 'address_original'},
        desc='Rename sh residential_address to address_original')

    dfl.addColumns(
        dataset='shareholders_residential_addresses',
        columns={'address_type': 'residential'},
        desc='Add address_type type column: res shs')

    # SHAREHOLDER POSTAL ADDRESSES

    dfl.dropColumns(
        dataset='shareholders_postal_addresses',
        colsToDrop=['residential_address'],
        desc='Drop the sh residential address from the postal table')

    dfl.renameColumns(
        dataset='shareholders_postal_addresses',
        columns={'postal_address': 'address_original'},
        desc='Rename sh postal_address to address_original')

    dfl.addColumns(
        dataset='shareholders_postal_addresses',
        columns={'address_type': 'postal'},
        desc='Add address_type type column: postal shs')

    # UNION SHAREHOLDER ADDRESSES

    dfl.union(
        datasets=[
            'shareholders_residential_addresses',
            'shareholders_postal_addresses'],
        targetDataset='shareholder_addresses',
        desc='Union the two shareholder address cols into a single dataset')

    dfl.addColumns(
        dataset='shareholder_addresses',
        columns={'src_table': 'SHAREHOLDERS'},
        desc='Add column to indicate source table for sh addresses')

    # SECRETARIES

    dfl.read(tableName='src_ipa_secretaries', dataLayer='SRC')

    # company_name,
    # company_number,
    # appointment_date,
    # name,
    # residential_address,
    # postal_address,
    # nationality,
    # this_person_has_consented_to_act_as_a_secretary_for_this_company,
    # appointed_date,
    # ceased,
    # this_person_has_consented_to_act_as_a_director_for_this_company,
    # residential_or_registered_office_address,
    # this_person_has_consented_to_act_as_a_shareholder_for_this_company,
    # appointed,

    dfl.dropColumns(
        dataset='src_ipa_secretaries',
        colsToKeep=[
            'company_name',
            'company_number',
            'appointed',
            'ceased',
            'residential_address',
            'postal_address'],
        desc='Drop unneeded columns from secretaries')

    dfl.renameColumns(
        dataset='src_ipa_secretaries',
        columns={
            'appointed': 'start_date',
            'ceased': 'end_date'},
        desc='Rename date columns on secretaries')

    dfl.duplicateDataset(
        dataset='src_ipa_secretaries',
        targetDatasets=[
            'secretaries_residential_addresses',
            'secretaries_postal_addresses'],
        desc='Duplicate secretaries into two datasets, ready to union')

    # SECRETARY RESIDENTIAL ADDRESSES

    dfl.dropColumns(
        dataset='secretaries_residential_addresses',
        colsToDrop=['postal_address'],
        desc='Drop the postal address from the sec residential table')

    dfl.renameColumns(
        dataset='secretaries_residential_addresses',
        columns={'residential_address': 'address_original'},
        desc='Rename sec residential_address to address_original')

    dfl.addColumns(
        dataset='secretaries_residential_addresses',
        columns={'address_type': 'residential'},
        desc='Add address_type type column: res secs')

    # SECRETARY POSTAL ADDRESSES

    dfl.dropColumns(
        dataset='secretaries_postal_addresses',
        colsToDrop=['residential_address'],
        desc='Drop the sec residential address from the postal table')

    dfl.renameColumns(
        dataset='secretaries_postal_addresses',
        columns={'postal_address': 'address_original'},
        desc='Rename sec postal_address to address_original')

    dfl.addColumns(
        dataset='secretaries_postal_addresses',
        columns={'address_type': 'postal'},
        desc='Add address_type type column: postal secs')

    # UNION SECRETARY ADDRESSES

    dfl.union(
        datasets=[
            'secretaries_residential_addresses',
            'secretaries_postal_addresses'],
        targetDataset='secretary_addresses',
        desc='Union the two secretary address cols into a single dataset')

    dfl.addColumns(
        dataset='secretary_addresses',
        columns={'src_table': 'SECRETARIES'},
        desc='Add column to indicate source table for sec addresses')

    # ODS_ADDRESSES

    # address_original
    # address_cleaned
    # address_type
    # start_date
    # end_date
    # company_name
    # company_number
    # src_table

    dfl.union(
        datasets=[
            'src_ipa_addresses',
            'director_addresses',
            'shareholder_addresses',
            'secretary_addresses'],
        targetDataset='ods_addresses',
        desc='Union the four datasets')

    dfl.cleanColumn(
        dataset='ods_addresses',
        cleaningFunc=cleanAddress,
        column='address_original',
        cleanedColumn='address_cleaned',
        desc="Clean the addresses")

    dfl.write(
        dataset='ods_addresses',
        targetTableName='ods_addresses',
        dataLayerID='STG')


def loadPostsToODS(scheduler):

    dfl = betl.DataFlow(desc='Extract posts and load to ods')

    dfl.read(tableName='src_wp_documents', dataLayer='SRC')

    dfl.dropColumns(
        dataset='src_wp_documents',
        colsToDrop=[
            'src_id',
            'post_id',
            'page_content_vector',
            'is_new'],
        desc='Drop unneeded columns from documents')

    dfl.renameColumns(
        dataset='src_wp_documents',
        columns={'id_src': 'nk_post_id',
                 'post_content': 'corruption_doc_content',
                 'post_name': 'corruption_doc_name',
                 'post_date': 'corruption_doc_date',
                 'post_title': 'corruption_doc_title',
                 'post_status': 'post_status',
                 'post_type': 'post_type'},
        desc='Rename columns ("post_" -> "corruption_")')

    dfl.dedupe(dataset='src_wp_documents',
               desc='Remove multiple copies of the same posts (WordPress ' +
                    'post versions, perhaps?)')

    dfl.addColumns(
        dataset='src_wp_documents',
        columns={
            'corruption_doc_status': dfl.getColumns(
                                        dataset='src_wp_documents',
                                        columnNames='post_status')},
        desc='Create a corruption_doc_status, preserving the original post ' +
             'status')

    dfl.cleanColumn(
        dataset='src_wp_documents',
        cleaningFunc=cleanPostContent,
        column='corruption_doc_content',
        cleanedColumn='corruption_doc_content_cleaned',
        desc="Clean the post content")

    dfl.write(
        dataset='src_wp_documents',
        targetTableName='ods_posts',
        dataLayerID='STG')


def loadSrcLinksToODS(scheduler):

    dfl = betl.DataFlow(
        desc='Start with all the people that are shareholders/directors/' +
             'secretaries from ods_people')

    # ODS_PEOPLE

    dfl.read(tableName='ods_people', dataLayer='STG')

    dfl.dropColumns(
        dataset='ods_people',
        colsToKeep=[
            'person_name_original',
            'person_name_cleaned',
            'company_name_original',
            'company_name_cleaned',
            'appointment_date',
            'ceased',
            'role_type'],  # used by setP2CRelationship
        desc='Drop cols to make datasets compatible')

    dfl.renameColumns(
        dataset='ods_people',
        columns={
            'person_name_original': 'origin_node_original',
            'person_name_cleaned': 'origin_node_cleaned',
            'company_name_original': 'target_node_original',
            'company_name_cleaned': 'target_node_cleaned',
            'appointment_date': 'start_date',
            'ceased': 'end_date'},
        desc='Rename ods_people columns')

    dfl.addColumns(
        dataset='ods_people',
        columns={'link_type': 'P2C',
                 'origin_node_type': 'person',
                 'target_node_type': 'company'},
        desc='Set these as all Person2Company (P2C) link_type')

    dfl.addColumns(
        dataset='ods_people',
        columns={'relationship': setP2CRelationship},
        desc='Set the relationship for each link')

    dfl.dropColumns(
        dataset='ods_people',
        colsToDrop={'role_type'},
        desc='Drop role_type')

    # COMPANY SHAREHOLDERS (ODS_COMPANIES)

    dfl.read(tableName='ods_companies', dataLayer='STG')

    dfl.filter(
        dataset='ods_companies',
        filters={'is_shareholder': 'YES'},
        desc='Filter to shareholder companies')

    dfl.dropColumns(
        dataset='ods_companies',
        colsToKeep=[
            'company_name_original',
            'company_name_cleaned',
            'company_shares_held_name_original',
            'company_shares_held_name_cleaned',
            'appointment_date',
            'ceased'],
        desc='Drop cols')

    dfl.renameColumns(
        dataset='ods_companies',
        columns={
            'company_name_original': 'origin_node_original',
            'company_name_cleaned': 'origin_node_cleaned',
            'company_shares_held_name_original': 'target_node_original',
            'company_shares_held_name_cleaned': 'target_node_cleaned',
            'appointment_date': 'start_date',
            'ceased': 'end_date'},
        desc='Rename ods_companies columns')

    dfl.addColumns(
        dataset='ods_companies',
        columns={
            'link_type': 'C2C',
            'relationship': 'sh_of',
            'origin_node_type': 'company',
            'target_node_type': 'company'},
        desc='Add link_type (C2C) and relationship (sh_of) columns')

    # ODS_SRC_LINKS

    dfl.union(
        datasets=[
            'ods_people',
            'ods_companies'],
        targetDataset='ods_src_links',
        desc='Union the two datasets')

    dfl.write(
        dataset='ods_src_links',
        targetTableName='ods_src_links',
        dataLayerID='STG')


#####################
# UTILITY FUNCTIONS #
#####################

def cleanCompanyName(colToClean):

    cleanCol = colToClean.str.upper()

    # Replace double spaces with single
    cleanCol.str.replace('\s+', ' ')

    return cleanCol


def cleanPersonName(colToClean):

    cleanCol = colToClean.str.upper()

    # Replace double spaces with single
    cleanCol = cleanCol.str.replace('\s+', ' ')

    # Replace back ticks with single quote
    cleanCol = cleanCol.str.replace('`', "'")

    # Replace &#8217; (apostrophe) with single quote
    cleanCol = cleanCol.str.replace('&#8217;', "'")

    return cleanCol


def cleanAddress(colToClean):

    # TODO: Fairly sure there's more to this!
    cleanCol = colToClean.str[0:]

    return cleanCol


def cleanPostContent(colToClean):

    cleanCol = colToClean.str.upper()

    # Remove line breaks
    cleanCol = cleanCol.str.replace('\n', ' ')
    cleanCol = cleanCol.str.replace('\r', ' ')

    # Replace back ticks with single quote
    cleanCol = cleanCol.str.replace('`', "'")

    # Replace &#8217; (apostrophe) with single quote
    cleanCol = cleanCol.str.replace('&#8217;', "'")

    return cleanCol


def validateStringDates(dateCol):
    dateCol = pd.to_datetime(dateCol, errors='coerce').dt.strftime('%Y%m%d') \
        .copy()
    dateCol.replace('NaT', '', inplace=True)
    return dateCol


def setP2CRelationship(row):
    if row['role_type'] == 'DIRECTOR':
        return 'd_of'
    elif row['role_type'] == 'SHAREHOLDER':
        return 'sh_of'
    elif row['role_type'] == 'SECRETARY':
        return 's_of'
