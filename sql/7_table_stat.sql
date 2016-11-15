-- table and query stats:
-- queries:	person 5700		tv_program 518		book 425	film 396  		computer_videogame 209 		album 378
-- tuples:	person 2M		tv_program 51K	 	book 2M		film 213k		computer_videogame 41K		album 867K		media 3M	tbl_all 5M

select semantic_type, count(*) as freq from query group by semantic_type order by freq desc;
select count(*) from person;
select DISTINCT TABLE_NAME, CARDINALITY from INFORMATION_SCHEMA.statistics ORDER BY CARDINALITY DESC; 
select DISTINCT TABLE_NAME, CARDINALITY from INFORMATION_SCHEMA.statistics where TABLE_NAME LIKE '%album%' ORDER BY CARDINALITY DESC; 

select count(*) from tbl_all; -- 5M
select count(*) from media; -- 3M
select count(*) from tbl_person; -- 2M