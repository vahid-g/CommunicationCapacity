select count(*) from tbl_tv_program;
select count(*) from tbl_query where semantic_type = 'tv_program';

# selects tv_program queries
select tbl_query.id, tbl_query.text as query, tbl_tv_program.name as tv_program, tbl_tv_program.description from tbl_query, tbl_tv_program 
	where tbl_query.fbid = tbl_tv_program.fbid;

# selects the hard queries
select tbl_query.id, tbl_query.text as query, tbl_tv_program.name as film, tbl_tv_program.description from tbl_query, tbl_tv_program 
	where tbl_query.fbid = tbl_tv_program.fbid and replace(tbl_query.text, CHAR(13), '') <> lcase(tbl_tv_program.name);

# subsamples the table to 1/10th of it
drop table tbl_tv_program_1;
create table tbl_tv_program_1 as
	select * from tbl_tv_program where mod(id, 10) < 1;

# selects all tv_program queries
select replace(tbl_query.text, CHAR(13), '') from tbl_query where tbl_query.semantic_type='tv_program';
select tbl_query.id, tbl_query.text as query, tbl_tv_program.name as name, tbl_tv_program.description 
	from tbl_query, tbl_tv_program  where tbl_query.fbid = tbl_tv_program.fbid;
