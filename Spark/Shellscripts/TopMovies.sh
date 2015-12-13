hadoop fs -rm -r TopMovies
spark-submit \
	--class MovieLens.TopMovies \
	--deploy-mode cluster \
	--master yarn \
	--executor-memory 512M \
	--num-executors 8 \
	TopMovies.jar \
	100 \
	MovieLens/movies.csv \
	MovieLens/ratings.csv \
        TopMovies
