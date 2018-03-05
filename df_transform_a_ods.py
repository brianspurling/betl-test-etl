import betl
import pandas as pd


def loadCompaniesToODS(scheduler):

    df_c = betl.readDataFromCsv('src_ipa_companies')

    betl.logStepStart('Add column: company_name_cleaned', 1)
    # TODO: #56 - apply proper cleaning logic, same for people.company_name
    df_c['company_name_cleaned'] = df_c['company_name'].str[0:]
    betl.logStepEnd(df_c)

    betl.logStepStart('Rename column: company_name to company_name_original',
                      2)
    df_c.rename(index=str,
                columns={'company_name': 'company_name_original'},
                inplace=True)
    betl.logStepEnd(df_c)

    betl.writeDataToCsv(df_c, 'ods_companies')

    del df_c


def loadPeopleToODS(scheduler):

    # We have people in directors, shareholders, and secretaries source tables,
    # So need to get them all into one dataset

    # Get the directors
    df_d = betl.readDataFromCsv('src_ipa_directors')

    betl.logStepStart('Add column: src_table', 1)
    df_d['src_table'] = 'DIRECTORS'
    betl.logStepEnd(df_d)

    # Get the shareholders

    # src_ipa_shareholders holds both people and companies. There is an
    # is_company flag (all 0 or 1), and a shareholding_company_number
    # (mostly '')
    # Some rows are flagged as a company, but have no company number.
    # But, vv, every row with a company number is flagged
    # TODO: what shall we do with these flags?
    # DQ TASK: implement some rules to identify additional companies
    # based on the name (e.g. contains LIMITED), and flag them, so
    # they get picked up by the filter below

    df_sh = betl.readDataFromCsv('src_ipa_shareholders')
    betl.logStepStart('Add column: src_table', 2)
    df_sh['src_table'] = 'SHAREHOLDERS'
    betl.logStepEnd(df_sh)

    # Get the secretaries
    df_s = betl.readDataFromCsv('src_ipa_secretaries')

    betl.logStepStart('Add column: src_table', 3)
    df_s['src_table'] = 'SECRETARIES'
    betl.logStepEnd(df_s)

    # Concatenate the three datasets

    #  Our target list of columns, common across all three datasets:
    cols = [
        'name',
        'company_name',
        'appointment_date',
        'ceased',
        'residential_address',
        'postal_address',
        'nationality',
        'role_type']

    betl.logStepStart('Drop cols to make datasets compatible (directors)', 5)
    cols_to_drop = [
        'company_number',
        'this_person_has_consented_to_act_as_a_director_for_this_company']
    df_d.drop(cols_to_drop, axis=1, inplace=True)
    df_d['role_type'] = 'DIRECTOR'
    df_d = df_d[cols]
    betl.logStepEnd(df_d)

    betl.logStepStart('Drop cols to make datasets compatible (shareholder)', 5)
    cols_to_drop = [
        'company_number',
        'shareholder_is_also_a_director',
        'residential_or_registered_office_address',
        'this_person_has_consented_to_act_as_a_shareholder_for_this_comp',
        'shareholding_company_number',
        'is_company',
        'place_of_incorporation',
        'this_company_has_consented_to_act_as_a_shareholder_for_this_com',
        'this_entity_has_consented_to_act_as_a_shareholder_for_this_comp',
        'company_name_or_number',
        'this_person_has_consented_to_act_as_a_director_for_this_company',
        'appointment_date']
    df_sh.drop(cols_to_drop, axis=1, inplace=True)
    df_sh.rename(index=str,
                 columns={'appointed': 'appointment_date'},
                 inplace=True)
    df_sh['role_type'] = 'SHAREHOLDER'
    df_sh = df_sh[cols]
    betl.logStepEnd(df_sh)

    betl.logStepStart('Drop cols to make datasets compatible (secretaries)', 5)
    cols_to_drop = [
        'company_number',
        'this_person_has_consented_to_act_as_a_secretary_for_this_compan',
        'appointed',
        'appointment_date',
        'this_person_has_consented_to_act_as_a_director_for_this_company',
        'this_person_has_consented_to_act_as_a_shareholder_for_this_comp',
        'residential_or_registered_office_address']
    df_s.drop(cols_to_drop, axis=1, inplace=True)
    df_s.rename(index=str,
                columns={'appointed_date': 'appointment_date'},
                inplace=True)
    df_s['role_type'] = 'SECRETARY'
    df_s = df_s[cols]
    betl.logStepEnd(df_s)

    betl.logStepStart('Concatentate the three datasets', 4)
    df_p = pd.concat([df_d, df_sh, df_s])
    betl.logStepEnd(df_p)

    del [df_d, df_sh, df_s]

    # name and company_name are our node NKs, so we need to clean them to
    # ensure we treat different variations of the same name as the same node

    betl.logStepStart('Add column: person_name_cleaned', 5)
    # TODO: #56 - apply proper cleaning logic
    df_p['person_name_cleaned'] = df_p['name'].str[0:]
    betl.logStepEnd(df_p)

    betl.logStepStart('Rename column: name to person_name_original', 6)
    df_p.rename(index=str,
                columns={'name': 'person_name_original'},
                inplace=True)
    betl.logStepEnd(df_p)

    betl.logStepStart('Add column: company_name_cleaned', 7)
    # TODO: #56 - apply proper cleaning logic, same for people.company_name
    df_p['company_name_cleaned'] = df_p['company_name'].str[0:]
    betl.logStepEnd(df_p)

    betl.logStepStart(
        'Rename column: company_name to company_name_original', 8)
    df_p.rename(index=str,
                columns={'company_name': 'company_name_original'},
                inplace=True)
    betl.logStepEnd(df_p)

    # Write to file
    betl.writeDataToCsv(df_p, 'ods_people')

    del df_p


def loadAddressesToODS(scheduler):

    # We have addresses in the addresses table (company addresses),
    # and in directors, shareholders, and scretaries (people addresses)
    # There's no natural key - just the address text

    # Get the company addresses

    df_c_a = betl.readDataFromCsv('src_ipa_addresses')

    betl.logStepStart('Add column: src_table', 1)
    df_c_a['src_table'] = 'ADDRESSES'
    betl.logStepEnd(df_c_a)

    # Get the directors' addresses

    df_d = betl.readDataFromCsv('src_ipa_directors')

    betl.logStepStart('Concat the two directors'' addresses together', 2)
    df_d_a_r = df_d['residential_address'].to_frame()
    df_d_a_r.rename(index=str,
                    columns={'residential_address': 'address'},
                    inplace=True)
    df_d_a_p = df_d['postal_address'].to_frame()
    df_d_a_p.rename(index=str,
                    columns={'postal_address': 'address'},
                    inplace=True)
    df_d_a = pd.concat([df_d_a_r, df_d_a_p])
    betl.logStepEnd(df_d_a)

    del [df_d_a_r, df_d_a_p]

    betl.logStepStart('Add column: src_table', 3)
    df_d_a['src_table'] = 'DIRECTORS'
    betl.logStepEnd(df_d)

    # Get the shareholders' addresses

    df_sh = betl.readDataFromCsv('src_ipa_shareholders')

    betl.logStepStart('Concat the two shareholders'' addresses together', 4)
    df_sh_a_r = df_sh['residential_address'].to_frame()
    df_sh_a_r.rename(index=str,
                     columns={'residential_address': 'address'},
                     inplace=True)
    df_sh_a_p = df_sh['postal_address'].to_frame()
    df_sh_a_p.rename(index=str,
                     columns={'postal_address': 'address'},
                     inplace=True)
    df_sh_a = pd.concat([df_sh_a_r, df_sh_a_p])
    betl.logStepEnd(df_sh_a)

    del [df_sh_a_r, df_sh_a_p]

    betl.logStepStart('Add column: src_table', 5)
    df_d_a['src_table'] = 'SHAREHOLDERS'
    betl.logStepEnd(df_d)

    # Get the secretaries' addresses

    df_s = betl.readDataFromCsv('src_ipa_secretaries')

    betl.logStepStart('Concat the two shareholders'' addresses together', 6)
    df_s_a_r = df_s['residential_address'].to_frame()
    df_s_a_r.rename(index=str,
                    columns={'residential_address': 'address'},
                    inplace=True)
    df_s_a_p = df_s['postal_address'].to_frame()
    df_s_a_p.rename(index=str,
                    columns={'postal_address': 'address'},
                    inplace=True)
    df_s_a = pd.concat([df_s_a_r, df_s_a_p])
    betl.logStepEnd(df_s_a)

    del [df_s_a_r, df_s_a_p]

    betl.logStepStart('Add column: src_table', 7)
    df_s['src_table'] = 'SECRETARIES'
    betl.logStepEnd(df_s)

    # Concat all four dfs together

    betl.logStepStart('Concat all four dfs together', 8)
    df_a = pd.concat([df_c_a, df_d_a, df_sh_a, df_s_a])
    betl.logStepEnd(df_a)

    del [df_c_a, df_d_a, df_sh_a, df_s_a]

    # Add cleaned address column

    betl.logStepStart('Add column: address_cleaned', 9)
    # TODO: #56 - apply proper cleaning logic
    df_a['address_cleaned'] = df_a['address'].str[0:]
    betl.logStepEnd(df_a)

    betl.logStepStart('Rename column: address to address_original', 10)
    df_a.rename(index=str,
                columns={'address': 'address_original'},
                inplace=True)
    betl.logStepEnd(df_a)

    # Write to file

    betl.writeDataToCsv(df_a, 'ods_addresses')

    del df_a


def loadPostsToODS(scheduler):
    df_p = betl.readDataFromCsv('src_wp_documents')

    betl.logStepStart('Drop unneeded column and rename the rest', 1)
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
    betl.logStepEnd(df_p)

    betl.logStepStart('Create two additional columns and reorder', 2)
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
    betl.logStepEnd(df_p)

    betl.writeDataToCsv(df_p, 'ods_posts')

    del df_p


#
#
#
def loadLinksToODS(scheduler):

    betl.logStepStart('Create empty df_links', 1)
    cols = [
        'origin_node_original',
        'origin_node_cleaned',
        'origin_node_type',
        'target_node_original',
        'target_node_cleaned',
        'target_node_type',
        'start_date',
        'end_date',
        'link_type',
        'relationship']
    df_temp = pd.DataFrame(columns=cols)
    betl.logStepEnd(df_temp)
    betl.writeDataToCsv(df_temp, 'ods_links')

    # Basic roles #

    df = betl.readDataFromCsv('ods_people')
    betl.logStepStart('Add columns: link_type and relationship', 1)
    df['link_type'] = 'P2C'
    df['relationship'] = df.apply(setRelationship, axis=1)
    betl.logStepEnd(df)

    betl.logStepStart('Drop columns', 2)
    cols_to_drop = list(df.columns.values)
    cols_to_drop.remove('person_name_original')
    cols_to_drop.remove('person_name_cleaned')
    cols_to_drop.remove('company_name_original')
    cols_to_drop.remove('company_name_cleaned')
    cols_to_drop.remove('appointment_date')
    cols_to_drop.remove('ceased')
    cols_to_drop.remove('link_type')
    cols_to_drop.remove('relationship')
    df.drop(cols_to_drop, axis=1, inplace=True)
    betl.logStepEnd(df)

    betl.logStepStart('Rename columns', 3)
    df.rename(index=str,
              columns={
                'person_name_original': 'origin_node_original',
                'person_name_cleaned': 'origin_node_cleaned',
                'company_name_original': 'target_node_original',
                'company_name_cleaned': 'target_node_cleaned',
                'appointment_date': 'start_date',
                'ceased': 'end_date',
                'link_type': 'link_type',
                'relationship': 'relationship',
              },
              inplace=True)
    betl.logStepEnd(df)

    betl.logStepStart('Add origin/target node types', 4)
    df['origin_node_type'] = 'person'
    df['target_node_type'] = 'company'
    betl.logStepEnd(df)

    betl.logStepStart('Reorder columns', 5)
    df = df[cols]
    betl.logStepEnd(df)

    betl.writeDataToCsv(df=df, file_or_filename='ods_links', mode='a')

    # Flip basic roles #

    betl.logStepStart('Filter to only basic roles', 5)
    df.loc[df['relationship'].isin(['d_of', 's_of', 'sh_of'])]
    betl.logStepEnd(df)

    betl.logStepStart('Rename cols to swap origin/target over and reorder', 6)
    df.rename(index=str,
              columns={
                'origin_node_original': 'target_node_original',
                'origin_node_cleaned': 'target_node_cleaned',
                'origin_node_type': 'target_node_type',
                'target_node_original': 'origin_node_original',
                'target_node_cleaned': 'origin_node_cleaned',
                'target_node_type': 'origin_node_type'},
              inplace=True)
    df = df[cols]
    betl.logStepEnd(df)

    betl.logStepStart('Change values of link_type and relationship', 7)
    df['link_type'] = 'C2P'
    df['relationship'] = df.apply(
        lambda row: row.relationship.replace('of', 'by'), axis=1)
    betl.logStepEnd(df)

    betl.writeDataToCsv(df=df, file_or_filename='ods_links', mode='a')


def setRelationship(row):
    if row['role_type'] == 'DIRECTOR':
        return 'd_of'
    elif row['role_type'] == 'SHAREHOLDER':
        return 'sh_of'
    elif row['role_type'] == 'SECRETARY':
        return 's_of'
