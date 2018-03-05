import betl


def prepareFTLinks(scheduler):
    df = betl.readDataFromCsv('mrg_links')

    betl.logStepStart('Create NK columns', 1)

    df['nk_origin_node'] = (df['origin_node_type'] +
                            '_' + df['origin_node_cleaned'])
    df['nk_target_node'] = (df['target_node_type'] +
                            '_' + df['target_node_cleaned'])
    df.rename(index=str,
              columns={
                  'link_type': 'nk_link_type',
                  'relationship': 'nk_relationship',
                  'start_date': 'nk_start_date',
                  'end_date': 'nk_end_date'},
              inplace=True)

    df['nk_start_date'] = df.apply(
        lambda row: str(row.nk_start_date).replace('-', ''), axis=1)
    df['nk_end_date'] = df.apply(
        lambda row: str(row.nk_end_date).replace('-', ''), axis=1)

    betl.logStepEnd(df)

    betl.logStepStart('Drop unneeded columns', 2)
    df.drop([
        'origin_node_cleaned',
        'origin_node_type',
        'target_node_cleaned',
        'target_node_type'],
        axis=1,
        inplace=True)
    betl.logStepEnd(df)

    betl.logStepStart('Add duration DD', 3)
    df['dd_duration'] = 10  # TODO placeholder for now
    betl.logStepEnd(df)

    betl.writeDataToCsv(df, 'trg_ft_links')
