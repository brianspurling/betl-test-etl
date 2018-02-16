import config

import betl

import pandas as pd


#
# We only need companies once, I think, but for consistency, extract it all
# to csv first
#
def getCompanies():
    funcName = 'getCompanies'
    logStr = ''

    a = 'Get data from sql and write to csv'
    df_c = betl.readFromEtlDB('src_ipa_companies')
    logStr += betl.describeDF(funcName, a, df_c, 1)

    df_c.to_csv(config.TMP_DATA_PATH + 'companies.csv',
                index=False)
    del df_c

    return logStr


#
# We need the people data more than once (ods_people and ods_addresses)
# so get it into CSV here, for faster loading later
#
def getPeople():
    funcName = 'getPeople'
    logStr = ''

    a = 'Get directors from sql and write to csv'
    df_d = betl.readFromEtlDB('src_ipa_directors')
    logStr += betl.describeDF(funcName, a, df_d, 1)
    df_d.to_csv(config.TMP_DATA_PATH + 'directors.csv',
                index=False)
    del df_d

    a = 'Get shareholders from sql and write to csv'
    df_sh = betl.readFromEtlDB('src_ipa_shareholders')
    logStr += betl.describeDF(funcName, a, df_sh, 2)
    df_sh.to_csv(config.TMP_DATA_PATH + 'shareholders.csv',
                 index=False)
    del df_sh

    a = 'Get secretaries from sql and write to csv'
    df_s = betl.readFromEtlDB('src_ipa_secretaries')
    logStr += betl.describeDF(funcName, a, df_s, 3)
    df_s.to_csv(config.TMP_DATA_PATH + 'secretaries.csv',
                index=False)
    del df_s

    return logStr


#
# We need addresses data more than once (ods_addresses and ods_address_types)
# so get it into CSV here, for faster loading later
#
def getAddresses():
    funcName = 'getAddresses'
    logStr = ''

    a = 'Get data from sql and write to csv'
    df_a = betl.readFromEtlDB('src_ipa_addresses')
    logStr += betl.describeDF(funcName, a, df_a, 1)

    df_a.to_csv(config.TMP_DATA_PATH + 'addresses.csv',
                index=False)
    del df_a

    return logStr


#
#
#
def loadCompaniesToODS():
    funcName = 'loadCompaniesToODS'
    logStr = ''

    a = 'Get data from csv'
    df_c = pd.read_csv(config.TMP_DATA_PATH + 'companies.csv')
    logStr += betl.describeDF(funcName, a, df_c, 1)

    a = 'drop all columns except name & number, add cleaned name, rename cols'
    cols = list(df_c.columns.values)
    cols.remove('company_name')
    cols.remove('company_number')
    df_c.drop(cols, axis=1, inplace=True)
    logStr += betl.describeDF(funcName, a, df_c, 2)

    #
    # Add cleaned company_name column
    #
    a = 'Add cleaned company_name, rename cols'
    df_c.rename(index=str,
                columns={'company_name': 'company_name_original'},
                inplace=True)

    df_c['company_name_cleaned'] = df_c['company_name_original'].str[0:]
    df_c['merged_company_id'] = ""
    logStr += betl.describeDF(funcName, a, df_c, 3)

    #
    # Write to ods data layer
    #
    # to do #23
    eng = betl.getEtlDBEng()
    df_c.to_sql('ods_companies', eng,
                if_exists='replace',
                index=False)
    del df_c

    return logStr


#
# We have people in directors, shareholders, and secretaries source tables,
# So need to get them all into one dataset
#
def loadPeopleToODS():
    funcName = 'loadPeopleToODS'
    logStr = ''

    #
    # Get the directors
    #

    a = 'Get data from csv'
    df_d = pd.read_csv(config.TMP_DATA_PATH + 'directors.csv')
    logStr += betl.describeDF(funcName, a, df_d, 1)

    a = 'drop all columns except name, add src_table'
    cols = list(df_d.columns.values)
    cols.remove('name')
    df_d.drop(cols, axis=1, inplace=True)
    df_d['src_table'] = 'DIRECTORS'
    logStr += betl.describeDF(funcName, a, df_d, 2)

    #
    # Get the shareholders
    #

    a = 'Get data from csv'
    df_sh = pd.read_csv(config.TMP_DATA_PATH + 'shareholders.csv')
    logStr += betl.describeDF(funcName, a, df_sh, 3)

    # src_ipa_shareholders holds both people and companies. There is an
    # is_company flag (all 0 or 1), and a shareholding_company_number
    # (mostly '')
    # Some rows are flagged as a company, but have no company number.
    # But, vv, every row with a company number is flagged
    # TODO: what shall we do with these flags?

    # DQ TASK: implement some rules to identify additional companies
    # based on the name (e.g. contains LIMITED), and flag them, so
    # they get picked up by the filter below

    a = 'drop all columns except name, add src_table'
    cols = list(df_sh.columns.values)
    cols.remove('name')
    df_sh.drop(cols, axis=1, inplace=True)
    df_sh['src_table'] = 'SHAREHOLDERS'
    logStr += betl.describeDF(funcName, a, df_sh, 5)

    #
    # Get the secretaries
    #

    a = 'Get data from csv'
    df_s = pd.read_csv(config.TMP_DATA_PATH + 'secretaries.csv')
    logStr += betl.describeDF(funcName, a, df_s, 6)

    a = 'drop all columns except name, add src_table'
    cols = list(df_s.columns.values)
    cols.remove('name')
    df_s.drop(cols, axis=1, inplace=True)
    df_s['src_table'] = 'SECRETARIES'
    logStr += betl.describeDF(funcName, a, df_s, 7)

    #
    # Concatenate the three datasets
    #

    a = 'Concatentate the three datasets'
    df_p = pd.concat([df_d, df_sh, df_s])
    del [df_d, df_sh, df_s]
    logStr += betl.describeDF(funcName, a, df_p, 8)

    #
    # Add cleaned person_name column
    #
    a = 'Add cleaned person_name, rename cols'
    df_p.rename(index=str,
                columns={'name': 'person_name_original'},
                inplace=True)

    df_p['person_name_cleaned'] = df_p['person_name_original'].str[0:]
    df_p['merged_person_id'] = ""
    logStr += betl.describeDF(funcName, a, df_p, 9)

    #
    # Write to ods data layer
    #
    # to do #23
    eng = betl.getEtlDBEng()
    df_p.to_sql('ods_people', eng,
                if_exists='replace',
                index=False)
    del df_p

    return logStr


#
# We have addresses in the addresses table (company addresses),
# and in directors, shareholders, and scretaries (people addresses)
# There's no natural key - just the address text
#
def loadAddressesToODS():
    funcName = 'loadAddressesToODS'
    logStr = ''

    #
    # Get the company addresses
    #

    a = 'Get data from csv'
    df_c_a = pd.read_csv(config.TMP_DATA_PATH + 'addresses.csv')
    logStr += betl.describeDF(funcName, a, df_c_a, 1)

    a = 'drop all columns except the address, add src_table'
    cols = list(df_c_a.columns.values)
    cols.remove('address')
    df_c_a.drop(cols, axis=1, inplace=True)
    df_c_a['src_table'] = 'ADDRESSES'
    logStr += betl.describeDF(funcName, a, df_c_a, 2)

    #
    # Get the directors' addresses
    #

    a = 'Get data from csv'
    df_d = pd.read_csv(config.TMP_DATA_PATH + 'directors.csv')
    logStr += betl.describeDF(funcName, a, df_d, 3)

    a = 'drop all columns except the two address columns, add src_table'
    cols = list(df_d.columns.values)
    cols.remove('residential_address')
    cols.remove('postal_address')
    df_d.drop(cols, axis=1, inplace=True)
    df_d['src_table'] = 'DIRECTORS'
    logStr += betl.describeDF(funcName, a, df_d, 4)

    a = 'Concat the two addresses together into a single df'
    df_d_a_r = df_d['residential_address'].to_frame()
    df_d_a_r.rename(index=str,
                    columns={'residential_address': 'address'},
                    inplace=True)
    df_d_a_p = df_d['postal_address'].to_frame()
    df_d_a_p.rename(index=str,
                    columns={'postal_address': 'address'},
                    inplace=True)
    df_d_a = pd.concat([df_d_a_r, df_d_a_p])
    del [df_d_a_r, df_d_a_p]
    logStr += betl.describeDF(funcName, a, df_d_a, 5)

    #
    # Get the shareholders' addresses
    #

    a = 'Get data from csv'
    df_sh = pd.read_csv(config.TMP_DATA_PATH + 'shareholders.csv')
    logStr += betl.describeDF(funcName, a, df_sh, 6)

    a = 'drop all columns except the two address columns, add src_table'
    cols = list(df_sh.columns.values)
    cols.remove('residential_address')
    cols.remove('postal_address')
    df_sh.drop(cols, axis=1, inplace=True)
    df_sh['src_table'] = 'SHAREHOLDERS'
    logStr += betl.describeDF(funcName, a, df_sh, 7)

    a = 'Concat the two together'
    df_sh_a_r = df_sh['residential_address'].to_frame()
    df_sh_a_r.rename(index=str,
                     columns={'residential_address': 'address'},
                     inplace=True)
    df_sh_a_p = df_sh['postal_address'].to_frame()
    df_sh_a_p.rename(index=str,
                     columns={'postal_address': 'address'},
                     inplace=True)
    df_sh_a = pd.concat([df_sh_a_r, df_sh_a_p])
    del [df_sh_a_r, df_sh_a_p]
    logStr += betl.describeDF(funcName, a, df_sh_a, 8)

    #
    # Get the secretaries' addresses
    #

    a = 'Get data from csv'
    df_s = pd.read_csv(config.TMP_DATA_PATH + 'secretaries.csv')
    logStr += betl.describeDF(funcName, a, df_s, 9)

    a = 'drop all columns except the two address columns, add src_table'
    cols = list(df_s.columns.values)
    cols.remove('residential_address')
    cols.remove('postal_address')
    df_s.drop(cols, axis=1, inplace=True)
    df_s['src_table'] = 'SECRETARIES'
    logStr += betl.describeDF(funcName, a, df_s, 10)

    a = 'Concat the two together'
    df_s_a_r = df_s['residential_address'].to_frame()
    df_s_a_r.rename(index=str,
                    columns={'residential_address': 'address'},
                    inplace=True)
    df_s_a_p = df_s['postal_address'].to_frame()
    df_s_a_p.rename(index=str,
                    columns={'postal_address': 'address'},
                    inplace=True)
    df_s_a = pd.concat([df_s_a_r, df_s_a_p])
    del [df_s_a_r, df_s_a_p]
    logStr += betl.describeDF(funcName, a, df_s_a, 11)

    #
    # Concat all four dfs together
    #
    a = 'Concat all four dfs together'
    df_a = pd.concat([df_c_a, df_d_a, df_sh_a, df_s_a])
    del [df_c_a, df_d_a, df_sh_a, df_s_a]
    logStr += betl.describeDF(funcName, a, df_a, 12)

    #
    # Add cleaned address column
    #
    a = 'Add cleaned address, rename cols'
    df_a.rename(index=str,
                columns={'address': 'address_original'},
                inplace=True)

    df_a['address_cleaned'] = df_a['address_original'].str[0:]
    df_a['merged_address_id'] = ""
    logStr += betl.describeDF(funcName, a, df_a, 13)

    #
    # Write to ODS data layer
    #
    # to do #23
    eng = betl.getEtlDBEng()
    df_a.to_sql('ods_addresses', eng,
                if_exists='replace',
                index=False)
    del df_a

    return logStr


#
# Posts from the wordpress (WP) data source
#
def loadPostsToODS():
    funcName = 'loadPostsToODS'
    logStr = ''

    a = 'Get data from sql'
    df_p = betl.readFromEtlDB('src_wp_documents')
    logStr += betl.describeDF(funcName, a, df_p, 1)

    a = 'Drop unneeded column and rename the rest'
    df_p.drop(['id_src', 'post_id'], axis=1, inplace=True)
    df_p.rename(index=str,
                columns={'src_id': 'nk_post_id',
                         'post_content': 'corruption_doc_content',
                         'post_name': 'corruption_doc_name',
                         'post_date': 'corruption_doc_date',
                         'post_title': 'corruption_doc_title',
                         'post_status': 'post_status',
                         'post_type': 'post_type'},
                inplace=True)
    logStr += betl.describeDF(funcName, a, df_p, 2)

    a = 'Create two additional columns and reorder columns'
    df_p['corruption_doc_tsvector'] = df_p['corruption_doc_content']
    df_p['corruption_doc_status'] = df_p['post_status']
    cols = ['nk_post_id',
            'corruption_doc_content',
            'corruption_doc_tsvector',
            'corruption_doc_name',
            'corruption_doc_date',
            'corruption_doc_title',
            'corruption_doc_status',
            'post_status',
            'post_type']
    df_p = df_p[cols]
    logStr += betl.describeDF(funcName, a, df_p, 3)

    #
    # Write to ODS data layer
    #
    eng = betl.getEtlDBEng()
    df_p.to_sql('ods_posts', eng,
                if_exists='replace',
                index=False)
    del df_p

    return logStr


def dedupeCompanies():
    funcName = 'dedupeCompanies'
    logStr = ''

    #
    # Get the companies from the ods staging table
    #
    a = 'Get data from sql'
    df_c = betl.readFromEtlDB('ods_companies')
    logStr += betl.describeDF(funcName, a, df_c, 1)

    #
    # Dedupe
    #
    # We consider two identical names to be the same company
    a = 'Make unique on company_name_cleaned'
    df_c.drop(['company_number',
               'company_name_original',
               'merged_company_id'], axis=1, inplace=True)
    df_c.drop_duplicates(inplace=True)
    logStr += betl.describeDF(funcName, a, df_c, 2)

    #
    # Write to temp file
    #
    df_c.to_csv(config.TMP_DATA_PATH + 'companies_deduped.csv',
                index=False)
    del df_c

    return logStr


def dedupePeople():
    funcName = 'dedupePeople'
    logStr = ''

    #
    # Get the companies from the ods staging table
    #
    a = 'Get data from sql'
    df_p = betl.readFromEtlDB('ods_people')
    logStr += betl.describeDF(funcName, a, df_p, 1)

    #
    # Dedupe
    #
    # We consider two identical names to be the same person
    a = 'Make unique on person_name_cleaned'
    df_p.drop(['src_table',
               'person_name_original',
               'merged_person_id'], axis=1, inplace=True)
    df_p.drop_duplicates(inplace=True)
    logStr += betl.describeDF(funcName, a, df_p, 2)

    #
    # Write to temp file
    #
    df_p.to_csv(config.TMP_DATA_PATH + 'people_deduped.csv',
                index=False)
    del df_p

    return logStr


def dedupeAddresses():
    funcName = 'dedupeAddresses'
    logStr = ''

    #
    # Get the companies from the ods staging table
    #
    a = 'Get data from sql'
    df_a = betl.readFromEtlDB('ods_addresses')
    logStr += betl.describeDF(funcName, a, df_a, 1)

    #
    # Dedupe
    #
    # We consider two identical names to be the same person
    a = 'Make unique on person_name_cleaned'
    df_a.drop(['src_table',
               'address_original',
               'merged_address_id'], axis=1, inplace=True)
    df_a.drop_duplicates(inplace=True)
    logStr += betl.describeDF(funcName, a, df_a, 2)

    #
    # Write to temp file
    #
    df_a.to_csv(config.TMP_DATA_PATH + 'addresses_deduped.csv',
                index=False)
    del df_a

    return logStr


#
# Just a straight-through load from SRC to a prepared csv
#
def prepareDMLinkType():
    funcName = 'prepareDMLinkType'
    logStr = ''

    a = 'Get data from sql'
    df = betl.readFromEtlDB('src_dm_link_type')
    logStr += betl.describeDF(funcName, a, df, 1)

    df.to_csv(config.TMP_DATA_PATH + 'dm_link_type.csv',
              index=False)
    del df

    return logStr


#
# Just a straight-through load from SRC to a prepared csv
#
def prepareDMRelationship():
    funcName = 'prepareDMRelationship'
    logStr = ''

    a = 'Get data from sql'
    df = betl.readFromEtlDB('src_dm_relationship')
    logStr += betl.describeDF(funcName, a, df, 1)

    df.to_csv(config.TMP_DATA_PATH + 'dm_relationship.csv',
              index=False)
    del df

    return logStr


#
# We'll pull people and companies and concat them
#
def prepareDMNode():
    funcName = 'prepareDMNode'
    logStr = ''

    a = 'Get companies data from csv'
    df_c = pd.read_csv(config.TMP_DATA_PATH + 'companies_deduped.csv')
    logStr += betl.describeDF(funcName, a, df_c, 1)

    a = 'Rename column to name; add a node_type col'
    df_c.rename(index=str,
                columns={'company_name_cleaned': 'name'},
                inplace=True)
    df_c['node_type'] = 'company'
    logStr += betl.describeDF(funcName, a, df_c, 2)

    a = 'Get people data from csv'
    df_p = pd.read_csv(config.TMP_DATA_PATH + 'people_deduped.csv')
    logStr += betl.describeDF(funcName, a, df_p, 3)

    a = 'Rename column to name; add a node_type col'
    df_p.rename(index=str,
                columns={'person_name_cleaned': 'name'},
                inplace=True)
    df_p['node_type'] = 'person'
    logStr += betl.describeDF(funcName, a, df_p, 4)

    a = 'Concat companies and people'
    df_n = pd.concat([df_p, df_c])
    del [df_p, df_c]
    logStr += betl.describeDF(funcName, a, df_n, 5)

    # to do #24
    #
    # Write to temp file
    #

    df_n.to_csv(config.TMP_DATA_PATH + 'dm_node.csv',
                index=False)
    del df_n

    return logStr


def prepareDMCorruptionDoc():
    funcName = 'prepareDMCorruptionDoc'
    logStr = ''

    a = 'Get data from sql'
    df = betl.readFromEtlDB('ods_posts')
    logStr += betl.describeDF(funcName, a, df, 1)

    # TODO #39
    df['number_mentioned_nodes'] = 0
    df['number_mentioned_people'] = 0
    df['number_mentioned_companies'] = 0

    df.to_csv(config.TMP_DATA_PATH + 'dm_corruption_doc.csv',
              index=False)
    del df

    return logStr


def prepareDMAddress():
    funcName = 'prepareDMAddress'
    logStr = ''

    a = 'Get data from csv'
    df = pd.read_csv(config.TMP_DATA_PATH + 'addresses_deduped.csv')
    logStr += betl.describeDF(funcName, a, df, 1)

    a = 'Rename column'
    df.rename(index=str,
              columns={'address_cleaned': 'address'},
              inplace=True)

    df.to_csv(config.TMP_DATA_PATH + 'dm_address.csv',
              index=False)
    del df

    return logStr


def prepareDMAddressType():
    funcName = 'prepareDMAddressType'
    logStr = ''

    # We have the address_types for people as MSD, and we add to it
    # the address_types for companies
    a = 'Get people address_types from sql (MSD)'
    df_p_at = betl.readFromEtlDB('src_dm_address_type')
    logStr += betl.describeDF(funcName, a, df_p_at, 1)

    a = 'Get company address_types from csv'
    df_c_a = pd.read_csv(config.TMP_DATA_PATH + 'addresses.csv')
    logStr += betl.describeDF(funcName, a, df_c_a, 2)

    a = 'remove cols, rename, dedupe, add additional cols'
    cols = list(df_c_a.columns.values)
    cols.remove('address_type')
    df_c_a.drop(cols, axis=1, inplace=True)
    df_c_a.drop_duplicates(inplace=True)
    df_c_a['address_type_name'] = 'company: ' + df_c_a['address_type'].map(str)
    df_c_a['address_role'] = 'Company'
    cols = ['address_type_name',
            'address_type',
            'address_role']
    df_c_a = df_c_a[cols]
    logStr += betl.describeDF(funcName, a, df_c_a, 3)

    a = 'Concatentate the two datasets'
    df_at = pd.concat([df_p_at, df_c_a])
    del [df_p_at, df_c_a]
    logStr += betl.describeDF(funcName, a, df_at, 4)

    df_at.to_csv(config.TMP_DATA_PATH + 'dm_address_type.csv',
                 index=False)
    del df_at

    return logStr


#
# Just a straight-through load from SRC to a prepared csv
#
def prepareDMNetworkMetric():
    funcName = 'prepareDMNetworkMetric'
    logStr = ''

    a = 'Get data from sql'
    df = betl.readFromEtlDB('src_dm_network_metric')
    logStr += betl.describeDF(funcName, a, df, 1)

    df.to_csv(config.TMP_DATA_PATH + 'dm_network_metric.csv',
              index=False)
    del df

    return logStr
