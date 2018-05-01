select * from dm_address limit 3;
select * from dm_address_type limit 3;
select * from dm_corruption_doc limit 3;
select * from dm_date limit 3;
select * from dm_link_type limit 3;
select * from dm_network_metric limit 3;
select * from dm_node limit 3;
select * from dm_relationship limit 3;
select * from ft_links limit 3;

select max(address_id) from dm_address;
select max(address_type_id) from dm_address_type;
select max(corruption_doc_id) from dm_corruption_doc;
select max(date_id) from dm_date;
select max(link_type_id) from dm_link_type;
select max(network_metric_id) from dm_network_metric;
select max(node_id) from dm_node;
select max(relationship_id) from dm_relationship;
select max(id) from ft_links


select * from dm_address where address_id < 0 limit 3;
select * from dm_address_type where address_type_id < 0 limit 3;
select * from dm_corruption_doc where corruption_doc_id < 0 limit 3;
select * from dm_date where date_id < 0 limit 3;
select * from dm_link_type where link_type_id < 0 limit 3;
select * from dm_network_metric where network_metric_id < 0 limit 3;
select * from dm_node where node_id < 0 limit 3;
select * from dm_relationship where relationship_id < 0 limit 3;

select date_id, 
	   count(*)
from   dm_date 
group by date_id 
having count(*) > 1;

select link_type_id, 
	   count(*)
from   dm_link_type 
group by link_type_id 
having count(*) > 1;

select relationship_id, 
	   count(*)
from   dm_relationship 
group by relationship_id 
having count(*) > 1;

select node_id, 
	   count(*)
from   dm_node 
group by node_id 
having count(*) > 1;