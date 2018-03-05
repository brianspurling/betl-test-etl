import betl


def loadCompaniesToMRG(scheduler):

    df_c = betl.readDataFromCsv('ods_companies')

    # We consider two identical names to be the same company
    betl.logStepStart('Make unique on company_name_cleaned', 1)
    cols = list(df_c.columns.values)
    cols.remove('company_name_cleaned')
    df_c.drop(cols, axis=1, inplace=True)
    df_c.drop_duplicates(inplace=True)
    betl.logStepEnd(df_c)

    betl.writeDataToCsv(df_c, 'mrg_companies')

    del df_c


def loadPeopleToMRG(scheduler):

    df_p = betl.readDataFromCsv('ods_people')

    # We consider two identical names to be the same person
    betl.logStepStart('Make unique on person_name_cleaned', 1)
    cols = list(df_p.columns.values)
    cols.remove('person_name_cleaned')
    df_p.drop(cols, axis=1, inplace=True)
    df_p.drop_duplicates(inplace=True)
    betl.logStepEnd(df_p)

    betl.writeDataToCsv(df_p, 'mrg_people')

    del df_p


def loadAddressesToMRG(scheduler):

    df_a = betl.readDataFromCsv('ods_addresses')

    # We consider two identical names to be the same person
    betl.logStepStart('Make unique on address_cleaned', 1)
    cols = list(df_a.columns.values)
    cols.remove('address_cleaned')
    df_a.drop(cols, axis=1, inplace=True)
    df_a.drop_duplicates(inplace=True)
    betl.logStepEnd(df_a)

    betl.writeDataToCsv(df_a, 'mrg_addresses')

    del df_a


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
def loadLinksToMRG(scheduler):

    df_p = betl.readDataFromCsv('ods_links')

    betl.logStepStart('Remove original origin/target cols and dedupe', 1)
    df_p.drop(['origin_node_original', 'target_node_original'],
              axis=1,
              inplace=True)
    df_p.drop_duplicates(inplace=True)
    betl.logStepEnd(df_p)

    betl.writeDataToCsv(df_p, 'mrg_links')

    del df_p
