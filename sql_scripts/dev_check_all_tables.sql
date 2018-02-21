select * from dm_address limit 3;
select * from dm_address_type limit 3;
select * from dm_corruption_doc limit 3;
select * from dm_date limit 3;
select * from dm_link_type limit 3;
select * from dm_network_metric limit 3;
select * from dm_nodƒe limit 3;
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