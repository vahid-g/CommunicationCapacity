-- table and query stats:
-- person: 5700		tv_program: 518		book: 425	film: 396  		computer_videogame: 209 	album: 378
-- person: 2M		tv_program: 51K	 	book: 2M	film: 213k		computer_videogame: 41K		album: 867K

select semantic_type, count(*) as freq from query group by semantic_type order by freq desc;
select count(*) from person;
select DISTINCT TABLE_NAME, CARDINALITY from INFORMATION_SCHEMA.statistics ORDER BY CARDINALITY DESC; 
select DISTINCT TABLE_NAME, CARDINALITY from INFORMATION_SCHEMA.statistics where TABLE_NAME LIKE '%album%' ORDER BY CARDINALITY DESC; 