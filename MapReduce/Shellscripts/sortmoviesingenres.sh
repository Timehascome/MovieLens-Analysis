#!/bin/bash
# Action
hadoop fs -rm -r movieratingstitlesbygenreout/Action/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/Action \
           movieratingstitlesbygenreout/Action/sorted

# Adventure
hadoop fs -rm -r movieratingstitlesbygenreout/Adventure/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/Adventure \
           movieratingstitlesbygenreout/Adventure/sorted

# Animation
hadoop fs -rm -r movieratingstitlesbygenreout/Animation/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/Animation \
           movieratingstitlesbygenreout/Animation/sorted

# Children
hadoop fs -rm -r movieratingstitlesbygenreout/Children/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/Children \
           movieratingstitlesbygenreout/Children/sorted

# Comedy
hadoop fs -rm -r movieratingstitlesbygenreout/Comedy/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/Comedy \
           movieratingstitlesbygenreout/Comedy/sorted

# Crime
hadoop fs -rm -r movieratingstitlesbygenreout/Crime/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/Crime \
           movieratingstitlesbygenreout/Crime/sorted

# Documentary
hadoop fs -rm -r movieratingstitlesbygenreout/Documentary/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/Documentary \
           movieratingstitlesbygenreout/Documentary/sorted

# Drama
hadoop fs -rm -r movieratingstitlesbygenreout/Drama/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/Drama \
           movieratingstitlesbygenreout/Drama/sorted

# Fantasy
hadoop fs -rm -r movieratingstitlesbygenreout/Fantasy/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/Fantasy \
           movieratingstitlesbygenreout/Fantasy/sorted

# Film-Noir
hadoop fs -rm -r movieratingstitlesbygenreout/Film-Noir/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/Film-Noir \
           movieratingstitlesbygenreout/Film-Noir/sorted

# Horror
hadoop fs -rm -r movieratingstitlesbygenreout/Horror/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/Horror \
           movieratingstitlesbygenreout/Horror/sorted

# IMAX
hadoop fs -rm -r movieratingstitlesbygenreout/IMAX/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/IMAX \
           movieratingstitlesbygenreout/IMAX/sorted

# Musical
hadoop fs -rm -r movieratingstitlesbygenreout/Musical/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/Musical \
           movieratingstitlesbygenreout/Musical/sorted

# Mystery
hadoop fs -rm -r movieratingstitlesbygenreout/Mystery/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/Mystery \
           movieratingstitlesbygenreout/Mystery/sorted

# None
hadoop fs -rm -r movieratingstitlesbygenreout/None/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/None \
           movieratingstitlesbygenreout/None/sorted

# Romance
hadoop fs -rm -r movieratingstitlesbygenreout/Romance/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/Romance \
           movieratingstitlesbygenreout/Romance/sorted

# Sci-Fi
hadoop fs -rm -r movieratingstitlesbygenreout/Sci-Fi/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/Sci-Fi \
           movieratingstitlesbygenreout/Sci-Fi/sorted

# Thriller
hadoop fs -rm -r movieratingstitlesbygenreout/Thriller/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/Thriller \
           movieratingstitlesbygenreout/Thriller/sorted

# War
hadoop fs -rm -r movieratingstitlesbygenreout/War/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/War \
           movieratingstitlesbygenreout/War/sorted

# Western
hadoop fs -rm -r movieratingstitlesbygenreout/Western/sorted
hadoop jar sortmoviesbyratingwithingenre.jar \
	   movielens.SortMoviesByRatingWithinGenreDriver \
           movieratingstitlesbygenreout/Western \
           movieratingstitlesbygenreout/Western/sorted

