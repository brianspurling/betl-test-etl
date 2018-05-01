select upper(n_o.name) origin_name,
       upper(n_t.name) target_name,
       upper(n_c.name) commonality_name,
       lt.link_type_name,
       r.relationship_name,
       d_s."cal_date" start_date,
       d_e."cal_date" end_date
from   ft_links ft
left join dm_node n_o on ft.fk_origin_node = n_o.node_id
left join dm_node n_t on ft.fk_target_node = n_t.node_id
left join dm_node n_c on ft.fk_commonality_node = n_c.node_id
left join dm_link_type lt on ft.fk_link_type = lt.link_type_id
left join dm_relationship r on ft.fk_relationship = r.relationship_id
left join dm_date d_s on ft.fk_start_date = d_s.date_id
left join dm_date d_e on ft.fk_end_date = d_e.date_id
where r.relationship_code = 'd=d'
and upper(n_o.name) = 'FEI ZHENG'
and upper(n_t.name) = 'ZUXING WENG'
order by upper(n_o.name) collate "C" desc,upper(n_t.name)  collate "C" desc,3,4,5,6
limit 200;



select * from dm_date where cal_date = '1976-05-17'
select * from dm_relationship

select r.relationship_name, lt.link_type_name,  count(*) 
from   ft_links ft
left join dm_relationship r on ft.fk_relationship = r.relationship_id
left join dm_link_type lt on ft.fk_link_type = lt.link_type_id
group by r.relationship_name, r.relationship_code, lt.link_type_name