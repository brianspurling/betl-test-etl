select n_o.name origin_name,
       n_t.name target_name,
       n_c.name commonality_name,
       lt.link_type_name,
       r.relationship_name,
       d_s."date" start_date,
       d_e."date" end_date
from   ft_links ft
left join dm_node n_o on ft.origin_node_fk = n_o.id
left join dm_node n_t on ft.target_node_fk = n_t.id
left join dm_node n_c on ft.commonality_node_fk = n_c.id
left join dm_link_type lt on ft.link_type_fk = lt.id
left join dm_relationship r on ft.relationship_fk = r.id
left join dm_date d_s on ft.start_date_fk = d_s.id
left join dm_date d_e on ft.end_date_fk = d_e.id
where r.relationship_name = 'Directors at the same company'
and 	  upper(n_o.name) = 'FEI ZHENG'
and upper(n_t.name) = 'ZUXING WENG'
order by n_o.name collate "C" desc, n_t.name collate "C" desc,3,4,5,6
limit 100;

select *
from   src_directors s
where  upper(s.name) in ('FEI ZHENG', 'ZUXING WENG')

select *
from   src_shareholders sh 
where  upper(sh.name) = 'THE FLETCHER CONSTRUCTION COMPANY LTD'


select r.relationship_name, lt.link_type_name, count(*)
from   ft_links ft
left join dm_relationship r on ft.relationship_fk = r.id
left join dm_link_type lt on ft.link_type_fk = lt.id
group by r.relationship_name, lt.link_type_name

