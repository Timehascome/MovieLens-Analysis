hadoop fs -rm -r TopGenres
spark-submit \
	--class MovieLens.TopGenres \
	--deploy-mode cluster \
	--master yarn \
	--executor-memory 512M \
	--num-executors 8 \
	TopGenres.jar \
	MovieLens/movies.csv \
	MovieLens/ratings.csv \
        TopGenres
