-- select queries and answers with special nationality
select tbl_query.id, tbl_query.text, tbl_person.name from tbl_query, tbl_statement, tbl_person where 
	tbl_query.fbid = tbl_person.fbid and 
	tbl_query.id = tbl_statement.query and tbl_statement.attribute like 'person_description' and value in 
	('africa', 'african', 'america', 'american', 'australian', 'brazilian', 'china', 'colombian', 'cuban', 'dutch', 'egyption', 'france', 'french', 
	'german', 'indian', 'iraqi', 'israeli', 'italian', 'italy', 'jamaica', 'jamaican', 'japanese', 'jordanian', 'korea', 'mexican');

-- ------
-- CVG --
select vi_game.genre, count(*) from vi_game group by genre;

-- select queries and answers from cvg with 'game' in their queries
select tbl_query.id, tbl_query.text, vi_game.id as game_id, vi_game.name, vi_game.genre
	from tbl_query, vi_game where semantic_type like 'computer_videogame' and text like '%game%'
		and tbl_query.fbid = vi_game.fbid;

-- select queries with cvg_genre attribute
SELECT * FROM querycapacity.tbl_query where id in
	(select query from tbl_statement where attribute like 'cvg_genre');

-- select answer genres of the queries with cvg_genre attribute
select * from tbl_game_genre where id in (
	select genre from tbl_game_game_genre where game in (
		select id from tbl_game where fbid in ( 
			select fbid from tbl_query where id in (
				select query from tbl_statement where attribute like 'cvg_genre'))));

-- select queries and answers for game_genre queries
select tbl_query.*, tbl_game.id as game_id, tbl_game.name as game_name, tbl_game_genre.name from tbl_query, tbl_game, tbl_game_game_genre, tbl_game_genre
	where tbl_query.id in (43944, 44513, 44909, 46140, 46479, 49413) and 
		tbl_query.fbid = tbl_game.fbid and tbl_game.id = tbl_game_game_genre.game and tbl_game_game_genre.genre = tbl_game_genre.id;

drop view vi_game;
create view vi_game as select tbl_game.id, tbl_game.fbid, tbl_game.name, tbl_game.description, tbl_game_genre.name as genre
	from tbl_game, tbl_game_game_genre, tbl_game_genre
	where tbl_game.id = tbl_game_game_genre.game and tbl_game_game_genre.genre = tbl_game_genre.id;

-- select query and answer by answer genre and query text
select tbl_query.id, text, vi_game.id as cvg_id, vi_game.name, vi_game.genre
	from tbl_query, vi_game 
	where semantic_type like 'computer_videogame' and tbl_query.fbid = vi_game.fbid
		and (text like '%game%' or text like '%computer%' or text like '%video%');

-- --------------
-- tv_prgoram ---
-- --------------
create view vi_tv_program as select tbl_tv_program.id, tbl_tv_program.fbid, tbl_tv_program.name, tbl_tv_genre.name as genre 
	from tbl_tv_program, tbl_tv_program_tv_genre, tbl_tv_genre 
	where tbl_tv_program.id = tbl_tv_program_tv_genre.tv_program and tbl_tv_program_tv_genre.tv_genre = tbl_tv_genre.id;

select attribute, count(*) from freebase3.tbl_statement group by attribute; 
select * from tbl_statement where attribute like 'tv_program_genre';
select value, count(*) from tbl_statement where attribute like 'tv_program_genre' group by value;

-- select query and answer by answer genre and query text
select tbl_query.id, text, vi_tv_program.id as tvp_id, vi_tv_program.name, vi_tv_program.genre
	from tbl_query, vi_tv_program 
	where semantic_type like 'tv_program' and tbl_query.fbid = vi_tv_program.fbid
		and (text like '%movie%' or text like '%film%' or text like '%tv%' or text like '%television%');

-- --------------
-- opera/play ---
-- --------------
create view vi_opera as select tbl_opera.id, tbl_opera.fbid, tbl_opera.name, tbl_opera_genre.name as genre 
	from tbl_opera, tbl_opera_opera_genre, tbl_opera_genre 
	where tbl_opera.id = tbl_opera_opera_genre.opera and tbl_opera_opera_genre.opera_genre = tbl_opera_genre.id;

drop view vi_play;
create view vi_play as select tbl_play.id, tbl_play.fbid, tbl_play.name, tbl_play_genre.name as genre 
	from tbl_play, tbl_play_play_genre, tbl_play_genre
	where tbl_play.id = tbl_play_play_genre.play and tbl_play_play_genre.play_genre = tbl_play_genre.id;

select tbl_query.*, vi_opera.* from tbl_query, vi_opera 
	where tbl_query.semantic_type like 'opera' and tbl_query.fbid = vi_opera.fbid and 
		text like '%opera%';

select tbl_query.id, tbl_query.text, vi_play.* from tbl_query, vi_play 
	where tbl_query.semantic_type like 'play' and tbl_query.fbid = vi_play.fbid and 
	text  regexp 'play|theme|musical';

