select attribute, count(*) from tbl_statement group by attribute;
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

