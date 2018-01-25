import config

import betl

import pandas as pd


#
#
#
def getAllCompanies():
    funcName = 'getAllCompanies'
    logStr = ''

    a = 'Get data from sql'
    conn = betl.getEtlDBConnection()
    df_c = pd.read_sql('SELECT * FROM ' + 'src_ipa_companies', con=conn)
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
def getAllPeople():
    funcName = 'getAllPeople'
    logStr = ''

    conn = betl.getEtlDBConnection()

    #
    # Get the directors
    #

    a = 'Get data from sql'
    df_d = pd.read_sql('SELECT * FROM ' + 'src_ipa_directors', con=conn)
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

    a = 'Get data from sql'
    df_sh = pd.read_sql('SELECT * FROM ' + 'src_ipa_shareholders', con=conn)
    logStr += betl.describeDF(funcName, a, df_sh, 3)

    # src_ipa_shareholders holds both people and companies. There is an
    # is_company flag (all 0 or 1), and a shareholding_company_number
    # (mostly '')
    # Some rows are flagged as a company, but have no company number.
    # But, vv, every row with a company number is flagged
    # So we filter out any that tell us they are a company

    # DQ TASK: implement some rules to identify additional companies
    # based on the name (e.g. contains LIMITED), and flag them, so
    # they get picked up by the filter below
    df_sh_p = df_sh.loc[(df_sh['is_company'] == 0) &
                        (df_sh['shareholding_company_number'] == '')]
    del df_sh
    logStr += betl.describeDF(funcName, a, df_sh_p, 4)

    a = 'drop all columns except name, add src_table'
    cols = list(df_sh_p.columns.values)
    cols.remove('name')
    df_sh_p.drop(cols, axis=1, inplace=True)
    df_sh_p['src_table'] = 'SHAREHOLDERS'
    logStr += betl.describeDF(funcName, a, df_sh_p, 5)

    #
    # Get the secretaries
    #

    a = 'Get data from sql'
    df_s = pd.read_sql('SELECT * FROM ' + 'src_ipa_secretaries', con=conn)
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
    df_p = pd.concat([df_d, df_sh_p, df_s])
    del [df_d, df_sh_p, df_s]
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
def getAllAddresses():
    funcName = 'getAllAddresses'
    logStr = ''

    conn = betl.getEtlDBConnection()

    #
    # Get the company addresses
    #

    a = 'Get data from sql'
    df_c_a = pd.read_sql('SELECT * FROM ' + 'src_ipa_addresses', con=conn)
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

    a = 'Get data from sql'
    df_d = pd.read_sql('SELECT * FROM ' + 'src_ipa_directors', con=conn)
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

    a = 'Get data from sql'
    df_sh = pd.read_sql('SELECT * FROM ' + 'src_ipa_shareholders', con=conn)
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

    a = 'Get data from sql'
    df_s = pd.read_sql('SELECT * FROM ' + 'src_ipa_secretaries', con=conn)
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


def dedupeCompanies():
    funcName = 'dedupeCompanies'
    logStr = ''

    conn = betl.getEtlDBConnection()

    #
    # Get the companies from the ods staging table
    #
    a = 'Get data from sql'
    df_c = pd.read_sql('SELECT * FROM ' + 'ods_companies', con=conn)
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
    df_c.to_csv(config.TMP_DATA_PATH + 'companies_deduped.csv')
    del df_c

    return logStr


def dedupePeople():
    funcName = 'dedupePeople'
    logStr = ''

    conn = betl.getEtlDBConnection()

    #
    # Get the companies from the ods staging table
    #
    a = 'Get data from sql'
    df_p = pd.read_sql('SELECT * FROM ' + 'ods_people', con=conn)
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

    conn = betl.getEtlDBConnection()

    #
    # Get the companies from the ods staging table
    #
    a = 'Get data from sql'
    df_a = pd.read_sql('SELECT * FROM ' + 'ods_addresses', con=conn)
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
# We'll pull people and companies, concat them, then add in the
# various other attribute that apply to both
#
def prepareDMNode():
    funcName = 'prepareDMNode'
    logStr = ''

    #
    # Prepare companies
    #

    a = 'Get data from csv'
    df_c = pd.read_csv(config.TMP_DATA_PATH + 'companies.csv')
    logStr += betl.describeDF(funcName, a, df_c, 1)

    a = 'Remove all the columns we do not want'
    cols = list(df_c.columns.values)
    cols.remove('company_name')
    cols.remove('company_number')
    df_c.drop(cols, axis=1, inplace=True)
    logStr += betl.describeDF(funcName, a, df_c, 2)

    # Rename columns to generic "node" naming
    # (note, company_number is our natural key)
    df_c.rename(index=str,
                columns={'company_number': 'node_key',
                         'company_name': 'node_name'},
                inplace=True)
    logStr += betl.describeDF(funcName, a, df_c, 3)

    a = 'Add a node_type column'
    df_c['node_type'] = 'company'
    logStr += betl.describeDF(funcName, a, df_c, 4)

    a = 'reorder the columns'
    df_c = df_c[['node_key', 'node_name', 'node_type']]
    logStr += betl.describeDF(funcName, a, df_c, 5)

    #
    # Prepare people
    #

    # Get data from csv
    df_p = pd.read_csv(config.TMP_DATA_PATH + 'people.csv')
    logStr += betl.describeDF(funcName, a, df_p, 6)

    # People have no proper natural key, we identify them based on their
    # name alone. So:
    a = 'Put the name into the key column. Urgh.'
    df_p['node_key'] = df_p['name']
    logStr += betl.describeDF(funcName, a, df_p, 7)

    a = 'Add a node_type column'
    df_p['node_type'] = 'person'
    logStr += betl.describeDF(funcName, a, df_p, 8)

    a = 'Rename columns'
    df_p.rename(index=str,
                columns={'name': 'node_name'},
                inplace=True)
    logStr += betl.describeDF(funcName, a, df_p, 9)

    a = 'reorder the columns'
    df_c = df_c[['node_key', 'node_name', 'node_type']]
    logStr += betl.describeDF(funcName, a, df_c, 10)

    #
    # Concat
    #

    a = 'Get data from csv'
    df_n = pd.concat([df_p, df_c])
    del [df_p, df_c]
    logStr += betl.describeDF(funcName, a, df_n, 11)

    # to do #24
    #
    # Write to temp file
    #

    df_n.to_csv(config.TMP_DATA_PATH + 'nodes.csv',
                index=False)
    del df_n

    return logStr


#
# Posts from the wordpress (WP) data source
#
def getPosts():
    funcName = 'getPosts'
    logStr = ''

    conn = betl.getEtlDBConnection()

    #
    # Prepare posts
    #

    a = 'Get data from sql'
    df_p = pd.read_sql('SELECT * FROM ' + 'src_wp_posts', con=conn)
    logStr += betl.describeDF(funcName, a, df_p, 1)

    #
    # Write to temp file
    #
    df_p.to_csv(config.TMP_DATA_PATH + 'post.csv')
    del df_p

    return logStr
