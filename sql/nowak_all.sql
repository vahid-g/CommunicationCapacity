-- building an aggregate table
drop table tbl_all;
create table tbl_all as 
	select p.id, p.fbid, p.name, p.description from tbl_person as p
		union (select tvp.id, tvp.fbid, tvp.name, tvp.description from tbl_tv_program as tvp)
		union (select f.id, f.fbid, f.name, f.description from tbl_film as f)
		union (select pl.id, pl.fbid, pl.name, pl.description from tbl_play as pl)
    union (select alb.id, alb.fbid, alb.name, alb.description from tbl_album as alb)
    union (select alb.id, alb.fbid, alb.name, alb.description from tbl_album as alb)
    union (select b.id, b.fbid, b.name, b.description from tbl_book as b)
		union (select g.id, g.fbid, g.name, g.description from tbl_game as g)
		union (select o.id, o.fbid, o.name, o.description from tbl_opera as o);

select count(*) from tbl_all;

-- selecting queries that have answer in tbl_all
select * from query where fbid in (select fbid from tbl_all);
create table query_all as select query.* from query inner join tbl_all on query.fbid = tbl_all.fbid;
drop table query_all;
create table query_all as select query.* from query, tbl_all where query.fbid = tbl_all.fbid;



select * from query where fbid in 
	(select fbid from tbl_all);
    
drop table qrel_all;
create table qrel_all as select query.*, tbl_all.name, tbl_all. 
	from query, tbl_all where query.fbid = tbl_all.fbid;
    