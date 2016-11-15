-- network decomposition
select tbl_statement.* from tbl_statement where tbl_statement.attribute like 'original_network';

-- create universal program view
drop view vi_tv_program_network;
create view vi_tv_program_network_base as select tbl_tv_program.*, tbl_tv_program_tv_network.tv_network , tbl_tv_network.name as network
	from tbl_tv_program inner join tbl_tv_program_tv_network on tbl_tv_program.id = tbl_tv_program_tv_network.tv_program 
		inner join tbl_tv_network on tbl_tv_program_tv_network.tv_network = tbl_tv_network.id;

drop view vi_tv_program_network;
create view vi_tv_program_network as select uv.id, uv.wikiid, uv.fbid, uv.name, uv.description, min(uv.tv_network) as nid, uv.network 
	from vi_tv_program_network_base uv group by uv.id; 

select * from vi_tv_program_network where network like '%espn%';

drop view vi_network_cbs;drop view vi_network_espn;drop view vi_network_fox;drop view vi_network_fx;
drop view vi_network_mtv;drop view vi_network_nbc;drop view vi_network_wb;
create view vi_network_cbs as select * from vi_tv_program_network where network like '%cbs%';
create view vi_network_espn as select * from vi_tv_program_network where network like '%espn%';
create view vi_network_fox as select * from vi_tv_program_network where network like '%fox%';
create view vi_network_fx as select * from vi_tv_program_network where network like '%fx%';
create view vi_network_mtv as select * from vi_tv_program_network where network like '%mtv%';
create view vi_network_nbc as select * from vi_tv_program_network where network like '%nbc%';
create view vi_network_wb as select * from vi_tv_program_network where network like '%wb%';

drop view vi_network_other;
create view vi_network_other as select vi_tv_program_network.* from vi_tv_program_network 
	left join vi_network_cbs on vi_tv_program_network.id = vi_network_cbs.id 
	left join vi_network_espn on vi_tv_program_network.id = vi_network_espn.id
	left join vi_network_fox on vi_tv_program_network.id = vi_network_fox.id
	left join vi_network_fx on vi_tv_program_network.id = vi_network_fx.id
	left join vi_network_mtv on vi_tv_program_network.id = vi_network_mtv.id
	left join vi_network_nbc on vi_tv_program_network.id = vi_network_nbc.id
	left join vi_network_wb on vi_tv_program_network.id = vi_network_wb.id
	where vi_network_cbs.id is null and vi_network_espn.id is null and vi_network_fox.id is null and vi_network_fx.id is null 
		and vi_network_mtv.id is null and vi_network_nbc.id is null and vi_network_wb.id is null;

drop view tmp_networks;
create view tmp_networks as select tbl_tv_network.id from tbl_tv_network where name like '%cbs%' OR name like '%espn%' OR name like '%fox%' OR 
	name like '%fx%' OR name like '%mtv%' OR name like '%nbc%' OR name like '%wb%';

drop view vi_network_other;
create view vi_network_other as select vi_tv_program_network.* from vi_tv_program_network 
	where vi_tv_program_network.nid not in (select * from tmp_networks);

select ((select count(*) from vi_network_other) 
	+ (select count(*) from vi_network_cbs)
	+ (select count(*) from vi_network_espn)
	+ (select count(*) from vi_network_fox)
	+ (select count(*) from vi_network_fx)
	+ (select count(*) from vi_network_mtv)
	+ (select count(*) from vi_network_nbc)
	+ (select count(*) from vi_network_wb)
	- (select count(*) from tbl_tv_program));

-- get answer tuples: proof that the minimum id in group by works for handling multiple valued networks
select st.value, st.query, pn.name, pn.description, pn.network from vi_tv_program_network_base pn, tbl_statement st, tbl_query qr 
	where st.attribute like 'original_network' and st.query = qr.id and qr.fbid = pn.fbid;

select st.value, st.query, pn.name, pn.description, pn.network from vi_tv_program_network pn, tbl_statement st, tbl_query qr 
	where st.attribute like 'original_network' and st.query = qr.id and qr.fbid = pn.fbid;



