hadoop fs -rm -r TopMoviesInGenres
spark-submit \
	--class MovieLens.TopMoviesInGenres \
	--deploy-mode cluster \
	--master yarn \
	--executor-memory 512M \
	--num-executors 8 \
	TopMoviesInGenres.jar \
        100 \
	MovieLens/movies.csv \
	MovieLens/ratings.csv \
        TopMoviesInGenres

