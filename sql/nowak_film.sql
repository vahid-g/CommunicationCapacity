
# selects the hard queries
select tbl_query.id, tbl_query.text as query, tbl_film.name as film, tbl_film.description from tbl_query, tbl_film 
	where tbl_query.fbid = tbl_film.fbid and replace(tbl_query.text, CHAR(13), '') <> lcase(tbl_film.name);

# subsamples the film table to 1/10th of it
drop table tbl_film_1;
create table tbl_film_1 as
	select * from tbl_film where mod(id, 10) < 1;

# selects all film queries
select replace(tbl_query.text, CHAR(13), '') from tbl_query where tbl_query.semantic_type='film';
select tbl_query.id, tbl_query.text as query, tbl_film.name as film, tbl_film.description 
	from tbl_query, tbl_film  where tbl_query.fbid = tbl_film.fbid;

select count(*) from tbl_query, tbl_film_9 as t2 where tbl_query.fbid = t2.fbid;

# create a table of films that contain just answer tuples
create table tbl_film_core as
	select tbl_film.* from tbl_film, tbl_query where tbl_query.fbid = tbl_film.fbid;

drop table tbl_film_notcore;
create table tbl_film_notcore as 
	select * from tbl_film where tbl_film.id not in (select id from tbl_film_core);

drop table tbl_film_periph;
create table tbl_film_periph as 
	select tbl_film.* from tbl_film left join tbl_film_core on tbl_film.id = tbl_film_core.id where tbl_film_core.id is null;

create table film_1 as 
	select * from tbl_film_core union select * from tbl_film_1;

select query.text, a.name, a.description from tbl_all as a, hard_film_queries, query 
	where query.id = hard_film_queries.id and query.fbid = a.fbid;

select query.* from query, hard_film_queries where query.id = hard_film_queries.id;