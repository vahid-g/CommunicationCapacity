drop table person;
# create a subset of person table
create table person as 
	select id, fbid, name, description from tbl_person limit 0, 210000;

select * from tbl_query, person where tbl_query.fbid = person.fbid;
drop table if exists category_tbl;

create table ftv as 
	(select tbl_film.id, tbl_film.fbid, tbl_film.name, tbl_film.description from tbl_film) 
		union (select tvp.id, tvp.fbid, tvp.name, tvp.description from tbl_tv_program as tvp);

select count(*), tbl_query.semantic_type from tbl_query group by semantic_type;

select q.id, q.text, p.name from tbl_query as q left join tbl_person as p 
		on q.fbid = p.fbid where q.semantic_type like 'person' limit 6000;

-- deleting all views
SET @views = NULL;
SELECT GROUP_CONCAT(table_schema, '.', table_name) INTO @views
 FROM information_schema.views 
 WHERE table_schema = 'querycapacity'; 

SET @views = IFNULL(CONCAT('DROP VIEW ', @views), 'SELECT "No Views"');
PREPARE stmt FROM @views;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- cleaning the tbl_query and making table query
drop table query;
create table query as select id, wiki_id, frequency, replace(text, char(13), '') as text, attributes_count, semantic_type, fbid from tbl_query;

-- finding hard queries
select q.id, q.text, p.name, p.description from query q, tbl_person p where q.fbid = p.fbid;
select q.id, q.text, p.name, p.description from query q, tbl_tv_program p 
	where q.fbid = p.fbid and q.text not like p.name;

-- building an aggregate table
create table tbl_all as 
	(select p.id, p.fbid, p.name, p.description from tbl_person as p)
		union (select tvp.id, tvp.fbid, tvp.name, tvp.description from tbl_tv_program as tvp)
		union (select b.id, b.fbid, b.name, b.description from tbl_book as b)
		union (select f.id, f.fbid, f.name, f.description from tbl_film as f)
		union (select pl.id, pl.fbid, pl.name, pl.description from tbl_play as pl)
		union (select g.id, g.fbid, g.name, g.description from tbl_game as g)
		union (select o.id, o.fbid, o.name, o.description from tbl_opera as o);

select count(*) from tbl_all;

-- selecting queries that have answer in tbl_all
select * from query where fbid in (select fbid from tbl_all);
create table query_all as select query.* from query inner join tbl_all on query.fbid = tbl_all.fbid;
drop table query_all;
create table query_all as select query.* from query, tbl_all where query.fbid = tbl_all.fbid;

