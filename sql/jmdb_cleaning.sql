create table movie_rating as 
select movies.title, ratings.votes, ratings.rank 
from movies, ratings
where movies.movieid = ratings.movieid;

SELECT 
    *
FROM
    movie_rating
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/mr.csv' 
FIELDS ENCLOSED BY '' 
TERMINATED BY ';' 
ESCAPED BY '' 
LINES TERMINATED BY '\n';

select count(*) from movie_rating;