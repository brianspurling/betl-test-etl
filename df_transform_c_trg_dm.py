import betl
import pandas as pd


def prepareDMLinkType(scheduler):
    df = betl.readData('src_msd_dm_link_type', 'SRC')
    betl.writeData(df, 'trg_dm_link_type', 'STG')
    del df


def prepareDMRelationship(scheduler):
    df = betl.readData('src_msd_dm_relationship', 'SRC')
    betl.writeData(df, 'trg_dm_relationship', 'STG')
    del df


def prepareDMNode(scheduler):
    df_c = betl.readData('mrg_companies', 'STG')

    betl.logStepStart('Rename column to name; add a node_type col', 1)
    df_c.rename(index=str,
                columns={'company_name_cleaned': 'name'},
                inplace=True)
    df_c['node_type'] = 'company'
    betl.logStepEnd(df_c)

    df_p = betl.readData('mrg_people', 'STG')

    betl.logStepStart('Rename column to name; add a node_type col', 2)
    df_p.rename(index=str,
                columns={'person_name_cleaned': 'name'},
                inplace=True)
    df_p['node_type'] = 'person'
    betl.logStepEnd(df_p)

    betl.logStepStart('Concat companies and people', 3)
    df_n = pd.concat([df_p, df_c])
    betl.logStepEnd(df_n)

    del [df_p, df_c]

    betl.writeData(df_n, 'trg_dm_node', 'STG')

    del df_n


def prepareDMCorruptionDoc(scheduler):
    df = betl.readData('ods_posts', 'STG')

    betl.logStepStart('Add 3 columns: number_mentioned_*', 1)
    # TODO #39
    df['number_mentioned_nodes'] = 0
    df['number_mentioned_people'] = 0
    df['number_mentioned_companies'] = 0
    betl.logStepEnd(df)

    betl.writeData(df, 'trg_dm_corruption_doc', 'STG')

    del df


def prepareDMAddress(scheduler):
    df = betl.readData('mrg_addresses', 'STG')

    betl.logStepStart('Rename column: address to address_cleaned', 1)
    df.rename(index=str,
              columns={'address_cleaned': 'address'},
              inplace=True)
    betl.logStepEnd(df)

    betl.writeData(df, 'trg_dm_address', 'STG')

    del df


def prepareDMAddressType(scheduler):
    # We have the address_types for people as
    # MSD, and we add to it the address_types for companies

    df_p_at = betl.readData('src_msd_dm_address_type', 'SRC')
    df_c_a = betl.readData('ods_addresses', 'STG')

    betl.logStepStart('Remove cols, rename, dedupe, add additional cols', 1)
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
    betl.logStepEnd(df_c_a)

    betl.logStepStart('Concatentate the two datasets', 2)
    df_at = pd.concat([df_p_at, df_c_a])
    betl.logStepEnd(df_at)

    del [df_p_at, df_c_a]

    betl.writeData(df_at, 'trg_dm_address_type', 'STG')

    del df_at


def prepareDMNetworkMetric(scheduler):
    df = betl.readData('src_msd_dm_network_metric', 'SRC')
    betl.writeData(df, 'trg_dm_network_metric', 'STG')
    del df
