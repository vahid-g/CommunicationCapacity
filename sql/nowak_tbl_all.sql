drop table tbl_all_rel;
create table tbl_all_rel as select tbl_all.*, tbl_query.text, tbl_query.frequency from tbl_all, tbl_query 
	where tbl_all.fbid = tbl_query.fbid;
    
drop table tbl_all_rel;
create table tbl_all_rel as select tbl_all.*, tbl_query.text, tbl_query.frequency from tbl_all, tbl_query 
	where tbl_all.fbid in (select fbid from tbl_query);    

create table tbl_all_nrel as select tbl_all.* from tbl_all
	where tbl_all.fbid not in (select fbid from tbl_all_rel);
    
select count(*) from tbl_all_rel;

select tbl_all_nrel.* from tbl_all_nrel, tbl_query where tbl_all_nrel.fbid = tbl_query.fbid;
	