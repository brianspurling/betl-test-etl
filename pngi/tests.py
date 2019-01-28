

    dfl.prepForLoad(
        dataset='ft_links',
        naturalKeyCols={
            'nk_origin_node': [
                'origin_node_type',
                'origin_node_cleaned'],
            'nk_target_node': [
                'target_node_type',
                'target_node_cleaned'],
            'nk_commonality_node': [
                'commonality_node_type',
                'commonality_node_cleaned'],
            'nk_link_type': 'link_type',
            'nk_relationship': 'relationship',
            'nk_start_date': 'start_date',
            'nk_end_date': 'end_date'})

FT_LINKS = 'select count(*) count ' +
           'from   mrg_src_links
FT_LINKS-FK_ORIGIN_NODE = '
