import betl
import pandas as pd


def loadCompaniesToODS(scheduler):

    # Load all the companies out of the companies table

    df_c = betl.readData('src_ipa_companies', 'SRC')

    betl.logStepStart('Drop all cols except name and number', 1)
    cols_to_drop = list(df_c)
    cols_to_drop.remove('company_number')
    cols_to_drop.remove('company_name')
    df_c.drop(cols_to_drop, axis=1, inplace=True)
    betl.logStepEnd(df_c)

    betl.logStepStart('Rename company_name to company_name_original', 2)
    df_c.rename(index=str,
                columns={'company_name': 'company_name_original'},
                inplace=True)
    betl.logStepEnd(df_c)

    betl.logStepStart('Add additional (blank) cols to match sh rows', 3)
    df_c['appointment_date'] = None
    df_c['ceased'] = None
    df_c['company_shares_held_name_original'] = None
    df_c['company_shares_held_name_cleaned'] = None
    df_c['company_shares_held_number'] = None
    df_c['is_shareholder'] = 'NO'
    betl.logStepEnd(df_c)

    # Load the companies that are shareholders out of the shareholders table
    # TODO: Not sure whether these companies are also in the src_ipa_companies
    # table, but we need to get them from src_ipa_shareholders anyway because,
    # like ods_people, we are really storing the _relationships_ in
    # ods_companies, not just the dimension. I should rename ods_companies and
    # ods_people to make this clearer.

    df_sh = betl.readData('src_ipa_shareholders', 'SRC')

    betl.logStepStart('Filter to is_company == 1', 4)
    df_sh = df_sh.loc[df_sh['is_company'] == '1']
    betl.logStepEnd(df_sh)

    betl.logStepStart('Add is_shareholder col', 5)
    df_sh['is_shareholder'] = 'YES'
    betl.logStepEnd(df_sh)

    betl.logStepStart('Drop cols to make datasets compatible', 6)
    cols_to_drop = list(df_sh)
    cols_to_drop.remove('name')
    cols_to_drop.remove('shareholding_company_number')
    cols_to_drop.remove('appointed')
    cols_to_drop.remove('ceased')
    cols_to_drop.remove('company_name')
    cols_to_drop.remove('company_number')
    cols_to_drop.remove('is_shareholder')
    df_sh.drop(cols_to_drop, axis=1, inplace=True)
    betl.logStepEnd(df_sh)

    betl.logStepStart('Rename cols', 7)
    df_sh.rename(index=str,
                 columns={'name': 'company_name_original',
                          'shareholding_company_number': 'company_number',
                          'company_name': 'company_shares_held_name_original',
                          'company_number': 'company_shares_held_number',
                          'appointed': 'appointment_date'},
                 inplace=True)
    betl.logStepEnd(df_sh)

    betl.logStepStart('Add column: company_shares_held_name_cleaned', 8)
    df_sh = cleanCompanyName(df_sh,
                             'company_shares_held_name_original',
                             'company_shares_held_name_cleaned')
    betl.logStepEnd(df_sh)

    # TODO this code is now repeated for ods_companies and ods_people
    # I think I should make ODS_companies and ods_people strictly dimensions
    # and move all this to ods_src_links. And then do links first, so that
    # ods_people can work off of that (to avoid repeating concat logic)
    betl.logStepStart('Convert dates to date types and back to string, ' +
                      'in the format YYYYMMDD', 9)
    df_sh['appointment_date'] = \
        pd.to_datetime(df_sh['appointment_date'],
                       errors='coerce').dt.strftime('%Y%m%d')
    df_sh['ceased'] = \
        pd.to_datetime(df_sh['ceased'],
                       errors='coerce').dt.strftime('%Y%m%d')
    df_sh['appointment_date'].replace('NaT', '', inplace=True)
    df_sh['ceased'].replace('NaT', '', inplace=True)
    betl.logStepEnd(df_sh)

    betl.logStepStart('Concatentate the two datasets', 10)
    df_both = pd.concat([df_c, df_sh])
    betl.logStepEnd(df_both)

    del df_c
    del df_sh

    betl.logStepStart('Add column: company_name_cleaned', 11)
    df_both = cleanCompanyName(df_both,
                               'company_name_original',
                               'company_name_cleaned')
    betl.logStepEnd(df_both)

    betl.writeData(df_both, 'ods_companies', 'STG')

    del df_both
    # TODO: I think I've been slack recently on dels. I should check all funcs


def loadPeopleToODS(scheduler):

    # We have people in directors, shareholders, and secretaries source tables,
    # So need to get them all into one dataset

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

    # Get the directors
    df_d = betl.readData('src_ipa_directors', 'SRC')

    betl.logStepStart('Add column: src_table', 1)
    df_d['src_table'] = 'DIRECTORS'
    betl.logStepEnd(df_d)

    betl.logStepStart('Drop cols to make datasets compatible (directors)', 2)
    cols_to_drop = [
        'company_number',
        'this_person_has_consented_to_act_as_a_director_for_this_company']
    df_d.drop(cols_to_drop, axis=1, inplace=True)
    betl.logStepEnd(df_d)

    betl.logStepStart('Assign role_type: DIRECTOR', 3)
    df_d['role_type'] = 'DIRECTOR'
    df_d = df_d[cols]
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

    df_sh = betl.readData('src_ipa_shareholders', 'SRC')

    betl.logStepStart('Filter to is_company == 0', 3)
    df_sh = df_sh.loc[df_sh['is_company'] == '0']
    betl.logStepEnd(df_sh)

    betl.logStepStart('Add column: src_table', 4)
    df_sh['src_table'] = 'SHAREHOLDERS'
    betl.logStepEnd(df_sh)

    betl.logStepStart('Drop cols to make datasets compatible (shareholder)', 5)
    cols_to_drop = [
        'company_number',
        'shareholder_is_also_a_director',
        'residential_or_registered_office_address',
        'this_person_has_consented_to_act_as_a_shareholder_for_this_company',
        'shareholding_company_number',
        'is_company',
        'place_of_incorporation',
        'this_company_has_consented_to_act_as_a_shareholder_for_this_company',
        'this_entity_has_consented_to_act_as_a_shareholder_for_this_company',
        'company_name_or_number',
        'this_person_has_consented_to_act_as_a_director_for_this_company',
        'appointment_date']
    df_sh.drop(cols_to_drop, axis=1, inplace=True)
    betl.logStepEnd(df_sh)

    betl.logStepStart('Rename appointed to appointment_date', 6)
    df_sh.rename(index=str,
                 columns={'appointed': 'appointment_date'},
                 inplace=True)
    betl.logStepEnd(df_sh)

    betl.logStepStart('Assign role_type: SHAREHOLDER', 7)
    df_sh['role_type'] = 'SHAREHOLDER'
    df_sh = df_sh[cols]
    betl.logStepEnd(df_sh)

    # Get the secretaries

    df_s = betl.readData('src_ipa_secretaries', 'SRC')

    betl.logStepStart('Add column: src_table', 8)
    df_s['src_table'] = 'SECRETARIES'
    betl.logStepEnd(df_s)

    betl.logStepStart('Drop cols to make datasets compatible (secretaries)', 9)
    cols_to_drop = [
        'company_number',
        'this_person_has_consented_to_act_as_a_secretary_for_this_company',
        'appointed',
        'appointment_date',
        'this_person_has_consented_to_act_as_a_director_for_this_company',
        'this_person_has_consented_to_act_as_a_shareholder_for_this_company',
        'residential_or_registered_office_address']
    df_s.drop(cols_to_drop, axis=1, inplace=True)
    betl.logStepEnd(df_s)

    betl.logStepStart('Rename appointed_date to appointment_date', 10)
    df_s.rename(index=str,
                columns={'appointed_date': 'appointment_date'},
                inplace=True)
    betl.logStepEnd(df_s)

    betl.logStepStart('Assign role_type: SECRETARY', 11)
    df_s['role_type'] = 'SECRETARY'
    df_s = df_s[cols]
    betl.logStepEnd(df_s)

    # Concatenate the three datasets

    betl.logStepStart('Concatentate the three datasets', 12)
    df_p = pd.concat([df_d, df_sh, df_s])
    betl.logStepEnd(df_p)

    del [df_d, df_sh, df_s]

    # name and company_name are our node NKs, so we need to clean them to
    # ensure we treat different variations of the same name as the same node

    betl.logStepStart('Rename column: name to person_name_original', 14)
    df_p.rename(index=str,
                columns={'name': 'person_name_original'},
                inplace=True)
    betl.logStepEnd(df_p)

    betl.logStepStart('Add column: person_name_cleaned', 13)
    df_p = cleanPersonName(df_p, 'person_name_original', 'person_name_cleaned')
    betl.logStepEnd(df_p)

    betl.logStepStart(
        'Rename column: company_name to company_name_original', 16)
    df_p.rename(index=str,
                columns={'company_name': 'company_name_original'},
                inplace=True)
    betl.logStepEnd(df_p)

    betl.logStepStart('Add column: company_name_cleaned', 15)
    df_p = cleanCompanyName(df_p,
                            'company_name_original',
                            'company_name_cleaned')
    betl.logStepEnd(df_p)

    # TODO: BETL Should report on out-of-bound dates, not just coerce them
    # to NaT
    betl.logStepStart('Convert dates to date types and back to string, ' +
                      'in the format YYYYMMDD', 14)
    df_p['appointment_date'] = \
        pd.to_datetime(df_p['appointment_date'],
                       errors='coerce').dt.strftime('%Y%m%d')
    df_p['ceased'] = \
        pd.to_datetime(df_p['ceased'],
                       errors='coerce').dt.strftime('%Y%m%d')
    df_p['appointment_date'].replace('NaT', '', inplace=True)
    df_p['ceased'].replace('NaT', '', inplace=True)
    betl.logStepEnd(df_p)

    # Write to file
    betl.writeData(df_p, 'ods_people', 'STG')

    del df_p


def cleanPersonName(df, nameToClean, targetCol):
    df[targetCol] = df[nameToClean].str.upper()
    # Replace double spaces with single
    df[targetCol] = \
        df[targetCol].str.replace('\s+', ' ')
    # Replace back ticks with single quote
    df[targetCol] = \
        df[targetCol].str.replace('`', "'")
    # Replace &#8217; (apostrophe) with single quote
    df[targetCol] = \
        df[targetCol].str.replace('&#8217;', "'")
    return df


def cleanCompanyName(df, nameToClean, targetCol):
    df[targetCol] = df[nameToClean].str.upper()
    # Replace double spaces with single
    df[targetCol] = \
        df[targetCol].str.replace('\s+', ' ')
    return df


def loadAddressesToODS(scheduler):

    # We have addresses in the addresses table (company addresses),
    # and in directors, shareholders, and scretaries (people addresses)
    # There's no natural key - just the address text

    # Get the company addresses

    df_c_a = betl.readData('src_ipa_addresses', 'SRC')

    betl.logStepStart('Add column: src_table', 1)
    df_c_a['src_table'] = 'ADDRESSES'
    betl.logStepEnd(df_c_a)

    # Get the directors' addresses

    df_d = betl.readData('src_ipa_directors', 'SRC')

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

    df_sh = betl.readData('src_ipa_shareholders', 'SRC')

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

    df_s = betl.readData('src_ipa_secretaries', 'SRC')

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

    betl.writeData(df_a, 'ods_addresses', 'STG')

    del df_a


def loadPostsToODS(scheduler):
    df_p = betl.readData('src_wp_documents', 'SRC')

    betl.logStepStart('Drop unneeded column and rename the rest', 1)
    df_p.drop(['src_id', 'post_id'], axis=1, inplace=True)
    df_p.rename(index=str,
                columns={'id_src': 'nk_post_id',
                         'post_content': 'corruption_doc_content',
                         'post_name': 'corruption_doc_name',
                         'post_date': 'corruption_doc_date',
                         'post_title': 'corruption_doc_title',
                         'post_status': 'post_status',
                         'post_type': 'post_type'},
                inplace=True)
    betl.logStepEnd(df_p)

    # It appears we have multiple copies of some of the posts (versions?)
    betl.logStepStart('Remove duplicates', 2)
    df_p.drop_duplicates(inplace=True)
    betl.logStepEnd(df_p)

    betl.logStepStart('Create two additional column and reorder', 3)
    df_p['corruption_doc_status'] = df_p['post_status']
    # TODO: remove all reordring on all talbes that are data model - auto
    # reordring should take care of this now
    cols = ['nk_post_id',
            'corruption_doc_content',
            'corruption_doc_name',
            'corruption_doc_date',
            'corruption_doc_title',
            'corruption_doc_status',
            'post_status',
            'post_type']
    df_p = df_p[cols]
    betl.logStepEnd(df_p)

    betl.writeData(df_p, 'ods_posts', 'STG', forceDBWrite=True)

    del df_p


def loadSrcLinksToODS(scheduler):

    # Start with all the people that are shareholders/directors/secretaries
    # from ods_people

    df_p = betl.readData('ods_people', 'STG')

    betl.logStepStart('Drop columns', 1)
    cols_to_drop = list(df_p.columns.values)
    cols_to_drop.remove('person_name_original')
    cols_to_drop.remove('person_name_cleaned')
    cols_to_drop.remove('company_name_original')
    cols_to_drop.remove('company_name_cleaned')
    cols_to_drop.remove('appointment_date')
    cols_to_drop.remove('ceased')
    cols_to_drop.remove('role_type')
    df_p.drop(cols_to_drop, axis=1, inplace=True)
    betl.logStepEnd(df_p)

    betl.logStepStart('Rename columns', 2)
    df_p.rename(index=str,
                columns={
                    'person_name_original': 'origin_node_original',
                    'person_name_cleaned': 'origin_node_cleaned',
                    'company_name_original': 'target_node_original',
                    'company_name_cleaned': 'target_node_cleaned',
                    'appointment_date': 'start_date',
                    'ceased': 'end_date'
                },
                inplace=True)
    betl.logStepEnd(df_p)

    betl.logStepStart('Add columns: link_type and relationship, remove ' +
                      'role_type', 3)
    df_p['link_type'] = 'P2C'
    df_p['relationship'] = df_p.apply(setP2CRelationship, axis=1)
    df_p.drop('role_type', axis=1, inplace=True)
    betl.logStepEnd(df_p)

    betl.logStepStart('Add origin/target node types', 4)
    df_p['origin_node_type'] = 'person'
    df_p['target_node_type'] = 'company'
    betl.logStepEnd(df_p)

    # Now get the companies that are shareholders from ods_companies

    df_c = betl.readData('ods_companies', 'STG')

    betl.logStepStart('Filter to is_shareholder == YES', 5)
    df_c = df_c.loc[df_c['is_shareholder'] == 'YES']
    betl.logStepEnd(df_c)

    betl.logStepStart('Drop columns', 6)
    cols_to_drop = list(df_c.columns.values)
    cols_to_drop.remove('company_name_original')
    cols_to_drop.remove('company_name_cleaned')
    cols_to_drop.remove('company_shares_held_name_original')
    cols_to_drop.remove('company_shares_held_name_cleaned')
    cols_to_drop.remove('appointment_date')
    cols_to_drop.remove('ceased')
    df_c.drop(cols_to_drop, axis=1, inplace=True)
    betl.logStepEnd(df_c)

    betl.logStepStart('Rename columns', 7)
    df_c.rename(index=str,
                columns={
                   'company_name_original': 'origin_node_original',
                   'company_name_cleaned': 'origin_node_cleaned',
                   'company_shares_held_name_original': 'target_node_original',
                   'company_shares_held_name_cleaned': 'target_node_cleaned',
                   'appointment_date': 'start_date',
                   'ceased': 'end_date',
                },
                inplace=True)
    betl.logStepEnd(df_c)

    betl.logStepStart('Add columns: link_type and relationship', 8)
    df_c['link_type'] = 'C2C'
    df_c['relationship'] = 'sh_of'
    betl.logStepEnd(df_c)

    betl.logStepStart('Add origin/target node types', 9)
    df_c['origin_node_type'] = 'company'
    df_c['target_node_type'] = 'company'
    betl.logStepEnd(df_c)

    betl.logStepStart('Concatentate the two datasets', 10)
    df_both = pd.concat([df_p, df_c])
    betl.logStepEnd(df_both)

    del df_p
    del df_c

    betl.logStepStart('Reorder columns', 11)
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
    df_both = df_both[cols]
    betl.logStepEnd(df_both)

    betl.writeData(df_both, 'ods_src_links', 'STG')

    del df_both


def setP2CRelationship(row):
    if row['role_type'] == 'DIRECTOR':
        return 'd_of'
    elif row['role_type'] == 'SHAREHOLDER':
        return 'sh_of'
    elif row['role_type'] == 'SECRETARY':
        return 's_of'
