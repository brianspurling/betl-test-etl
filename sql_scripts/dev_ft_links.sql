select n_o.name origin_name,
       n_t.name target_name,
       lt.link_type_name,
       r.relationship_name,
       d_s.calDate start_date,
       d_e.calDate end_date
from   ft_links ft
left join dm_node n_o on ft.fk_origin_node = n_o.node_id
left join dm_node n_t on ft.fk_target_node = n_t.node_id
left join dm_link_type lt on ft.fk_link_type = lt.link_type_id
left join dm_relationship r on ft.fk_relationship = r.relationship_id
left join dm_date d_s on ft.fk_start_date = d_s.date_id
left join dm_date d_e on ft.fk_end_date = d_e.date_id
where 1=1
and upper(n_o."name") like 'PETER O''NEILL%'
and lt.link_type_name in ('P2C','C2P')

select * from dm_date