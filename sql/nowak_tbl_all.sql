drop table tbl_all_rel;
create table tbl_all_rel as select tbl_all.*, tbl_query.text, tbl_query.frequency from tbl_all, tbl_query 
	where tbl_all.fbid = tbl_query.fbid;
    
drop table tbl_all_rel;
create table tbl_all_rel as select tbl_all.*, tbl_query.text, tbl_query.frequency from tbl_all, tbl_query 
	where tbl_all.fbid in (select fbid from tbl_query);    

create table tbl_all_nrel as select tbl_all.* from tbl_all
	where tbl_all.fbid not in (select fbid from tbl_all_rel);

select tbl_all_nrel.* from tbl_all_nrel, tbl_query where tbl_all_nrel.fbid = tbl_query.fbid;
	
create table qrel as select query.*, tbl_all.name, tbl_all.description 
	from query, tbl_all where query.fbid = tbl_all.fbid;    

drop table tbl_all;
create table tbl_all as
        select p.id, p.fbid, p.name, p.description, 'person' as `semantic_type` from tbl_person as p 
                union (select tvp.id, tvp.fbid, tvp.name, tvp.description, 'tv_program' as `semantic_type` from tbl_tv_program as tvp)
                union (select f.id, f.fbid, f.name, f.description, 'film' as `semantic_type` from tbl_film as f)
                union (select pl.id, pl.fbid, pl.name, pl.description, 'play' as `semantic_type` from tbl_play as pl)
                union (select al.id, al.fbid, al.name, al.description, 'album' as `semantic_type` from tbl_album as al)
                union (select b.id, b.fbid, b.name, b.description, 'book' as `semantic_type` from tbl_book as b)
                union (select g.id, g.fbid, g.name, g.description, 'game' as `semantic_type` from tbl_game as g)
                union (select o.id, o.fbid, o.name, o.description, 'opera' as `semantic_type` from tbl_opera as o);

select distinct semantic_type from query;