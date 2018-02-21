select n_o.name origin_name,
       n_t.name target_name,
       lt.link_type_name,
       r.relationship_name,
       d_s."date" start_date,
       d_e."date" end_date
from   ft_links ft
left join dm_node n_o on ft.origin_node_fk = n_o.id
left join dm_node n_t on ft.target_node_fk = n_t.id
left join dm_link_type lt on ft.link_type_fk = lt.id
left join dm_relationship r on ft.relationship_fk = r.id
left join dm_date d_s on ft.start_date_fk = d_s.id
left join dm_date d_e on ft.end_date_fk = d_e.id
where 1=1
and n_o.name like 'PETER O''NEILL%'
and lt.link_type_name in ('P2C','C2P')