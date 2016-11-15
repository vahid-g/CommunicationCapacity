-- film
create view vi_film as select tbl_film.*, tbl_film_genre.name as genre from tbl_film, tbl_film_film_genre, tbl_film_genre 
	where tbl_film.id = tbl_film_film_genre.film and tbl_film_film_genre.genre = tbl_film_genre.id;

drop view vi_film_dram;
create view vi_film_dram as select tbl_film.* from tbl_film
	where tbl_film.id in (select distinct id from vi_film where genre like '%comedy%' or genre like '%drama%');
select * from vi_film_dram;

drop view vi_film_other;
create view vi_film_other as select tbl_film.* from tbl_film
	where tbl_film.id not in (select id from vi_film_dram);

-- tv_program
drop view vi_tv_program;
create view vi_tv_program as select tbl_tv_program.*, tbl_tv_genre.name as genre 
	from tbl_tv_program, tbl_tv_program_tv_genre, tbl_tv_genre 
	where tbl_tv_program.id = tbl_tv_program_tv_genre.tv_program 
		and tbl_tv_program_tv_genre.tv_genre = tbl_tv_genre.id;

drop view vi_tvp_dram;
create view vi_tvp_dram as select tbl_tv_program.* from tbl_tv_program 
	where tbl_tv_program.id in (select distinct id from vi_tv_program where genre like '%comedy%' or genre like '%drama%');

drop view vi_tvp_other;
create view vi_tvp_other as select tbl_tv_program.* from tbl_tv_program
	where tbl_tv_program.id not in (select id from vi_tvp_dram);


-- computer_videogame
drop view vi_cvg_action_adv;
create view vi_cvg_action_adv as select tbl_game.* from tbl_game
	where tbl_game.id in (select distinct id from vi_game where genre like '%action%' or genre like '%adventure%');

drop view vi_cvg_other;
create view vi_cvg_other as select tbl_game.* from tbl_game
	where tbl_game.id not in (select id from vi_cvg_action_adv);

-- opera
drop view vi_opera_comique;
create view vi_opera_comique as select tbl_opera.* from tbl_opera
	where tbl_opera.id in (select distinct id from vi_opera where genre like '%comiq%');

drop view vi_opera_other;
create view vi_opera_other as select tbl_opera.* from tbl_opera
	where tbl_opera.id not in (select id from vi_opera_comique);

-- play
drop view vi_play;
create view vi_play as select tbl_play.*, tbl_play_genre.name as genre 
	from tbl_play, tbl_play_play_genre, tbl_play_genre 
	where tbl_play.id = tbl_play_play_genre.play and tbl_play_play_genre.play_genre = tbl_play_genre.id;

drop view vi_play_drama;
create view vi_play_drama as select tbl_play.* from tbl_play
	where tbl_play.id in (select distinct id from vi_play where genre like '%drama%' or genre like '%Tragedy%' or genre like '%Tragicomedy%');

drop view vi_play_other;
create view vi_play_other as select tbl_play.* from tbl_play
	where tbl_play.id not in (select id from vi_play_drama);

select * from vi_opera_comique;