import betl


def prepareFTLinks():
    df = betl.readFromCsv('mrg_links')

    betl.logStepStart('Rename columns', 1)
    df.rename(index=str,
              columns={
                  'origin_node_cleaned': 'origin_node',
                  'target_node_cleaned': 'target_node'},
              inplace=True)
    betl.logStepEnd(df)

    betl.logStepStart('Add duration DD', 2)

    # TODO placeholder for now
    df['duration'] = 10

    betl.writeToCsv(df, 'trg_ft_links')

    del df
