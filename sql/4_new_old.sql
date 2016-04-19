create table tbl_film_tvp_new as
(SELECT id,fbid,name,description FROM tbl_tv_program where 
air_date_of_first_episode REGEXP '2000|2001|2002|2003|2004|2005|2006|2007|2008|2009|2010|2011|2012|2013|2014') 
union
(SELECT id,fbid,name,description FROM tbl_film where 
initial_release_date REGEXP '2000|2001|2002|2003|2004|2005|2006|2007|2008|2009|2010|2011|2012|2013|2014'
);

SELECT count(*) FROM tbl_film where 
 initial_release_date REGEXP '2000|2001|2002|2003|2004|2005|2006|2007|2008|2009|2010|2011|2012|2013|2014';
SELECT count(*) FROM tbl_tv_program where 
 air_date_of_first_episode REGEXP '2000|2001|2002|2003|2004|2005|2006|2007|2008|2009|2010|2011|2012|2013|2014';

drop view vi_film_old;
create view vi_film_old as
(select id,fbid,name,description from tbl_film where fbid not in 
	(select fbid from tbl_film_tvp_new));

create table tbl_film_old as select * from vi_film_old;

create table tbl_tvp_old as
(select id,fbid,name,description from tbl_tv_program where fbid not in 
	(select fbid from tbl_film_tvp_new));

create table tbl_new_or_tvp as 
	(select id,fbid,name,description from tbl_tvp_old) union
		(select id,fbid,name,description from tbl_film_tvp_new);

create table tbl_new_or_film as 
	(select id,fbid,name,description from tbl_film_old) union
		(select id,fbid,name,description from tbl_film_tvp_new);

-- SELECT id,fbid,name,description,type FROM tbl_film where 
-- initial_release_date REGEXP '2000|2001|2002|2003|2004|2005|2006|2007|2008|2009|2010|2011|2012|2013|2014';
