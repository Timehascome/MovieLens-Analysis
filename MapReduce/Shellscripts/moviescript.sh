#!/bin/bash
# Orchestrates the Java MapReduce programs for movie data analysis.

# MR01 - Generates a file of tab separated (MovieId, MovieTitle, MovieGenres)
#     - Mapper only, no reducer necessary
hadoop fs -rm -r movietitlesgenresout
hadoop jar movietitlesgenresbyid.jar \
	   movielens.MovieTitlesGenresDriver \
           movietitles \
           movietitlesgenresout

# MR02 - Generates a file of (MovieId, RatingSum|RatingCount) pairs          
hadoop fs -rm -r movieratingsout
hadoop jar movieratingsbyid.jar \
           movielens.MovieRatingsDriver \
           -D mapred.reduce.tasks=1 \
           movieratings \
           movieratingsout

# MR03 - Joins output file from MR1 with output file from MR2
hadoop fs -rm -r movietitlesgenresratingsout
hadoop jar joinmovietitlesgenresratings.jar \
	   movielens.JoinMovieTitlesGenresRatingsDriver \
           -D mapred.reduce.tasks=1 \
           movietitlesgenresout \
           movieratingsout \
           movietitlesgenresratingsout
            
# MR04 - Calculates movie title ratings
hadoop fs -rm -r movietitleratingsout
hadoop jar calculatemovietitleratings.jar \
	   movielens.CalculateMovieTitleRatingsDriver \
	   -D mapred.reduce.tasks=1 \
           movietitlesgenresratingsout \
           movietitleratingsout

# MR05 - Sorts movie titles by rating in descending order
hadoop fs -rm -r sortedmovietitlesbyrating
hadoop jar sortmovietitlesbyrating.jar \
	   movielens.SortMovieTitlesByRatingDriver \
           movietitleratingsout \
           sortedmovietitlesbyrating

# MR06 - Calculates movie genre ratings
hadoop fs -rm -r moviegenreratingsout
hadoop jar calculatemoviegenreratings.jar \
	   movielens.CalculateMovieGenreRatingsDriver \
           movietitlesgenresratingsout \
           moviegenreratingsout

# MR07 - Sort movie genres by rating in descending order
hadoop fs -rm -r sortedmoviegenresbyrating
hadoop jar sortmoviegenresbyrating.jar \
	   movielens.SortMovieGenresByRatingDriver \
           moviegenreratingsout \
           sortedmoviegenresbyrating

# MR08 - Movie rating count by user id
hadoop fs -rm -r movieratingcountbyuseridout
hadoop jar movieratingcountbyuserid.jar \
	   movielens.MovieRatingCountDriver \
	   -D mapred.reduce.tasks=1 \
           movieratings \
           movieratingcountbyuseridout

# MR09 - User rating frequency
hadoop fs -rm -r movieratingfrequency
hadoop jar userratingfrequency.jar \
	   movielens.RatingFrequencyDriver \
	   -D mapred.reduce.tasks=1 \
           movieratingcountbyuseridout \
           movieratingfrequency

# MR10 - Sort User rating frequency
hadoop fs -rm -r sortedmovieratingfrequency
hadoop jar sortuserratingfrequency.jar \
           movielens.SortRatingFrequencyDriver \
           movieratingfrequency \
           sortedmovieratingfrequency

# MR11 - Movie Genres Titles Ratings
hadoop fs -rm -r moviegenrestitlesratingsout
hadoop jar moviegenrestitlesratings.jar \
	   movielens.MovieGenresTitlesRatingsDriver \
           -D mapred.reduce.tasks=1 \
           movietitlesgenresratingsout \
           moviegenrestitlesratingsout

# MR12 - Movie Ratings Titles by Genre directory
hadoop fs -rm -r movieratingstitlesbygenreout
hadoop jar movieratingstitlesbygenre.jar \
	   movielens.MovieRatingsTitlesByGenreDriver \
           moviegenrestitlesratingsout \
           movieratingstitlesbygenreout


