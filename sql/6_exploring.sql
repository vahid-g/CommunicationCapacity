select attribute, count(*) as 'freq' from tbl_statement group by attribute order by freq desc;
select * from tbl_statement where attribute like 'profession';
create table person_profession as 
	select prs.id, prs.fbid, prs.name as name, prs.description, prf.name as profession 
	from tbl_person as prs, tbl_person_profession as pp, tbl_profession as prf
		where prs.id = pp.person and pp.profession = prf.id;

select name, count(*) from person_profession group by name having count(*) > 1;

select tbl_query.text as `query_text`, tbl_statement.value as `profession_in_query`,  
		tbl_query.wiki_id, person_profession.name, person_profession.profession 
	from tbl_query inner join tbl_statement inner join person_profession 
		on tbl_query.id = tbl_statement.query and tbl_query.fbid = person_profession.fbid 
		where tbl_statement.attribute like 'profession';


-- -----------------------------------------------------
-- exploring tv_program with genre = cartoon series
-- -----------------------------------------------------

select * from tbl_statement where attribute like 'tv_program_genre';
select * from tbl_query where  attributes_count > 2;
select tq.id, wiki_id, text, semantic_type, attribute, value 
	from tbl_query as tq inner join tbl_statement as ts on tq.id = ts.query where tq.attributes_count > 1;
select semantic_type, count(*) from tbl_query group by semantic_type;

create view tvp_query as select * from tbl_query where semantic_type like 'tv_program';
select * from tvp_query;
create view tvp_queried as select tvp.* from tbl_tv_program as tvp, tvp_query as q 
	where tvp.fbid = q.fbid;
select tvq.name from tvp_queried as tvq;

select tvg.name, count(*) from tbl_tv_program as tvp, tbl_tv_program_tv_genre as tvp_tvg, tbl_tv_genre as tvg
	where tvp.id = tvp_tvg.tv_program and tvp_tvg.tv_genre = tvg.id group by tvg.name;

-- select relevant tuples with their related genre
select tvg.name, count(*) from tvp_queried as tvp, tbl_tv_program_tv_genre as tvp_tvg, tbl_tv_genre as tvg
	where tvp.id = tvp_tvg.tv_program and tvp_tvg.tv_genre = tvg.id group by tvg.name;

-- creating a table for cartoon series and a table for the rest of the tv_programs
drop table tvp_cartoon_series;
create table tvp_cartoon_series as select tvp.* from tbl_tv_program as tvp, tbl_tv_program_tv_genre as tvp_tvg, tbl_tv_genre as tvg
	where tvp.id = tvp_tvg.tv_program and tvp_tvg.tv_genre = tvg.id and tvg.name like 'Cartoon series';

drop table tvp_not_cartoon_series;
create table tvp_not_cartoon_series as select tvp.* from tbl_tv_program as tvp left join tvp_cartoon_series as tvp_cs
	on tvp.id = tvp_cs.id where tvp_cs.id is null;

create table tvp_genre as select tvp.*, tvg.name as genre from tbl_tv_program as tvp, tbl_tv_program_tv_genre as tvp_tvg, tbl_tv_genre as tvg
	where tvp.id = tvp_tvg.tv_program and tvp_tvg.tv_genre = tvg.id;

select count(*) from tvp_not_cartoon_series;
select count(*) from tbl_tv_program;

create view tvp_cartoon_query as select tbl_query.* from tbl_query, tvp_cartoon_series as tvp_cs where tbl_query.fbid = tvp_cs.fbid;

create table tvp_comedy_film as select tvp.* from tbl_tv_program as tvp, tbl_tv_program_tv_genre as tvp_tvg, tbl_tv_genre as tvg
	where tvp.id = tvp_tvg.tv_program and tvp_tvg.tv_genre = tvg.id and tvg.name like 'Comedy film';

create table tvp_not_comedy_film as select tvp.* from tbl_tv_program as tvp left join tvp_comedy_film as tvp_cf
	on tvp.id = tvp_cf.id where tvp_cf.id is null;

-- -------------------------------------------------------------
-- exploring queries with a keyword specifying the semantic_type
--  
-- 	tbl_tv_program size: 52k
-- 	tbl_film size: 209k
-- -------------------------------------------------------------
select * from query where semantic_type like 'tv_program' and text REGEXP 'program| tv| television| serie| show | show$| film | film$| movie';
select * from query where text REGEXP 'show | show$| movie$' and fbid in (select fbid from tbl_film);
select * from query where text REGEXP 'program| tv| television| serie| show | show$| film | film$| movie' and fbid in (select fbid from tbl_tv_program);
select * from query where semantic_type like 'tv_program' and text REGEXP 'film| movie';

drop table tmp_tvp; 
create table tmp_tvp as select id, fbid, name, description from tbl_tv_program;
create table tmp_film as select id, fbid, name, description from tbl_film;
create table tvp_film as select * from tmp_tvp union select * from tmp_film;
alter table tmp_tvp add semantic_type CHAR(255) default 'tv_program';
alter table tmp_film add semantic_type char(255) default 'film';


select q.text, tvp_film.name from tvp_film, query as q where tvp_film.fbid = q.fbid 
	-- and q.fbid in (select fbid from tbl_tv_program) 
	and q.text REGEXP 'program| tv| television| serie| show | show$| film | film$| movie';

select * from query where text regexp 'music|record|song|sound| art |album' and fbid in (select fbid from tbl_album);
select q.*, s.attribute, s.value from query as q, tbl_statement as s where q.id = s.query and s.attribute like "tv_program_description"
	and q.text regexp 'program| tv| television| serie| show | show$| film | film$| movie'
	and q.fbid in (select fbid from tbl_tv_program);

-- -------------------------------------------------------------
-- creating a huge media table
-- -------------------------------------------------------------
select count(*) from tbl_album where description is not null;
select count(*) from tbl_album;
create table tmp_album as select id, fbid, name, description from tbl_album;
create table tmp_book as select id, fbid, name, description from tbl_book;
create table tmp_game as select id, fbid, name, description from tbl_game;

alter table tmp_album add semantic_type CHAR(255) default 'album';
alter table tmp_book add semantic_type CHAR(255) default 'book';
alter table tmp_game add semantic_type CHAR(255) default 'computer_videogame';

drop table media;
create table media as (select * from tmp_film) union (select * from tmp_tvp)
	union (select * from tmp_album) union (select * from tmp_book) union (select * from tmp_game);

select count(*) from media where description is not null;
select count(*) from media;

create table media_query_tmp as select query.fbid from query 
	where semantic_type like 'film' OR semantic_type like 'tv_program' OR semantic_type like 'album' 
		OR semantic_type like 'album' OR semantic_type like 'book' OR semantic_type like 'computer_videgame';
        
drop table media_rels;

-- -------------------------------------------------------------
-- analyzing the queries that lose precision over media table
-- -------------------------------------------------------------
alter table media add fulltext (name);
select * from media where match(name) against('Lost');
select * from media where name like '%Frankenstein%';
select * from media where name like '%A Separate Peace%';

select count(*) from tbl_all; -- 5M
select count(*) from media; -- 3M
select count(*) from tbl_person; -- 2M
