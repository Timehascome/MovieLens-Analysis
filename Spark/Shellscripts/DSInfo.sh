hadoop fs -rm -r DSCounts
hadoop fs -rm -r GenreMovieCounts
hadoop fs -rm -r GenreMovieFreqDistribution
hadoop fs -rm -r RatingsFreqDistribution
hadoop fs -rm -r UserRatingActivityFreqDistribution
hadoop fs -rm -r AvgNbrRatingsPerUserId
hadoop fs -rm -r MinMaxNbrRatingsPerUserId
hadoop fs -rm -r AvgNbrTagsPerUserId
hadoop fs -rm -r UserTaggingActivityFreqDistribution
hadoop fs -rm -r MinMaxNbrTagsPerUserId
hadoop fs -rm -r MovieLensDSInfo
spark-submit \
	--class MovieLens.DataSetInfo \
	--deploy-mode cluster \
	--master yarn \
	--executor-memory 512M \
	--num-executors 8 \
	DSInfo.jar \
	MovieLens/movies.csv \
	MovieLens/ratings.csv \
        MovieLens/tags.csv
