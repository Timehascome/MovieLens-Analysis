-- Input File Record Counts
select count(distinct MovieID) as nbrMovies from dbo.Movietitles;
select count(*) as nbrMovies from dbo.MovieTitles;
select count(*) as nbrRatings from dbo.MovieRatings;
select count(*) as nbrTags from dbo.MovieTags;

-- Movie Count by Genre
select g1.Genre, count(*) as MovieCount
  from dbo.MovieTitles mv1
       inner join dbo.MovieGenreMapping map1
		       on mv1.MovieID = map1.MovieID
       inner join dbo.MovieGenres g1
		       on map1.GenreID = g1.GenreID
 group by g1.Genre
 order by g1.Genre

-- Genre Movie Frequency Distribution
select GenreCount, count(*) as Freq
  from ( 
		select mv1.MovieID, count(*) as GenreCount
		  from dbo.MovieTitles mv1
			   inner join dbo.MovieGenreMapping map1
					   on mv1.MovieID = map1.MovieID
			   inner join dbo.MovieGenres g1
					   on map1.GenreID = g1.GenreID
		 group by mv1.MovieID
	   ) t1
 group by GenreCount
 order by GenreCount

-- Ratings Frequency Distribution
select Rating, count(*) as Freq
  from dbo.MovieRatings
 group by Rating
 order by Rating desc

-- nbrUsersWithRatings
select count(distinct UserID) as nbrUsersWithRatings 
  from dbo.MovieRatings;

-- min, max, avg NbrRatings
select min(nbrRatings) as minNbrRatings,
       max(nbrRatings) as maxNbrRatings,
	   avg(nbrRatings) as avgNbrRatings
 from (
	   select UserID, count(*) as nbrRatings
		 from dbo.MovieRatings
	    group by UserID
	  ) t1

-- nbrUsersWithTags
select count(distinct UserID) as nbrUsersWithTags 
  from dbo.MovieTags;

-- min, max, avg NbrTags
select min(nbrTags) as minNbrTags,
       max(nbrTags) as maxNbrTags,
	   avg(nbrTags) as avgNbrTags
  from (
		select UserID, count(*) as nbrTags
		  from dbo.MovieTags
		 group by UserID
	   ) t1

-- User Frequency Distribution - Ratings
select nbrRatings, count(*) as userFreq
 from (
	   select UserID, count(*) as nbrRatings
		 from dbo.MovieRatings
	    group by UserID
	  ) t1
group by nbrRatings
order by nbrRatings

-- User Frequency Distribution - Tags
select nbrTags, count(*) as userFreq
 from (
	   select UserID, count(*) as nbrTags
		 from dbo.MovieTags
	    group by UserID
	  ) t1
group by nbrTags
order by nbrTags

-- Top 100 movies
select title, avg_rating, rating_cnt, rnk
  from (
		select mv1.Title, avg1.avg_rating, avg1.rating_cnt,
		       rank() over(order by avg1.avg_rating desc) as rnk
		  from (
				select MovieID, avg(Rating) as avg_rating, count(*) as rating_cnt
				  from dbo.MovieRatings
				 group by MovieID
			   ) avg1
			   inner join dbo.MovieTitles mv1
					   on avg1.MovieID = mv1.MovieID
		 where avg1.rating_cnt > 100
	   ) top100
	where rnk between 1 and 100
	order by rnk, Title

-- Genre Rankings
select g2.Genre, g2.avg_rating, g2.rating_cnt
  from (
        select g1.Genre, avg(Rating) as avg_rating, count(*) as rating_cnt
			from dbo.MovieRatings mr1
				inner join dbo.MovieTitles mv1
						on mr1.movieid = mv1.movieid
				inner join dbo.MovieGenreMapping map1
						on mv1.movieid = map1.movieid
				inner join dbo.MovieGenres g1
						on map1.genreid = g1.genreid
			group by g1.Genre
	   ) g2
 order by g2.avg_rating desc
 
 -- Top 10 Movies in Genres
 select top10.Genre, top10.Title, top10.avg_rating, top10.rating_cnt, top10.rnk 
   from (
		 select t1.Genre, t1.Title, t1.avg_rating, t1.rating_cnt,
				rank() over (partition by Genre order by avg_rating desc, Title) as rnk
		   from (
				select gen1.genre, mv1.title, avg1.avg_rating, avg1.rating_cnt
				  from (
						select MovieID, avg(Rating) as avg_rating, count(*) as rating_cnt
						  from dbo.MovieRatings
						 group by MovieID
					   ) avg1
					   inner join dbo.MovieTitles mv1
							   on avg1.MovieID = mv1.MovieID
					   inner join dbo.MovieGenreMapping map1
							   on avg1.MovieID = map1.MovieID
					   inner join dbo.moviegenres gen1
							   on map1.GenreID = gen1.GenreID
				 where avg1.rating_cnt > 100
				 ) t1
		) top10
  where top10.rnk <= 10
  order by top10.Genre, top10.rnk





 
 

					     





