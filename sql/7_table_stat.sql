-- table and query stats:
-- person: 5700		tv_program: 518		book: 425	film: 396 album: 378 computer_videogame: 209
-- PERSON: 2M		tv_program: 51K	 	book: 2M	film: 213k	

select semantic_type, count(*) as freq from query group by semantic_type order by freq desc;
select count(*) from person;
select DISTINCT TABLE_NAME, CARDINALITY from INFORMATION_SCHEMA.statistics ORDER BY CARDINALITY DESC;