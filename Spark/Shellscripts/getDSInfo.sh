rm MovieLensDataMetrics.txt
rm UserRatingActivityFreqDistribution.txt
rm UserTaggingActivityFreqDistribution.txt

echo "-----------------------------" >> MovieLensDataMetrics.txt
echo "" >> MovieLensDataMetrics.txt
echo "DataSet Record Counts" >> MovieLensDataMetrics.txt
hadoop fs -cat DSCounts/* >> MovieLensDataMetrics.txt
echo "" >> MovieLensDataMetrics.txt
echo "-----------------------------" >> MovieLensDataMetrics.txt
echo "" >> MovieLensDataMetrics.txt
echo "Genre Movie Counts" >> MovieLensDataMetrics.txt
hadoop fs -cat GenreMovieCounts/* >> MovieLensDataMetrics.txt
echo "" >> MovieLensDataMetrics.txt
echo "-----------------------------" >> MovieLensDataMetrics.txt
echo "" >> MovieLensDataMetrics.txt
echo "Genre Movie Frequency Distribution" >> MovieLensDataMetrics.txt
hadoop fs -cat GenreMovieFreqDistribution/* >> MovieLensDataMetrics.txt
echo "" >> MovieLensDataMetrics.txt
echo "-----------------------------" >> MovieLensDataMetrics.txt
echo "" >> MovieLensDataMetrics.txt
echo "Ratings Frequency Distribution" >> MovieLensDataMetrics.txt
hadoop fs -cat RatingsFreqDistribution/* >> MovieLensDataMetrics.txt
echo "" >> MovieLensDataMetrics.txt
echo "-----------------------------" >> MovieLensDataMetrics.txt
echo "" >> MovieLensDataMetrics.txt
echo "Avg Nbr Ratings Per User" >> MovieLensDataMetrics.txt
hadoop fs -cat AvgNbrRatingsPerUserId/* >> MovieLensDataMetrics.txt
echo "" >> MovieLensDataMetrics.txt
echo "-----------------------------" >> MovieLensDataMetrics.txt
echo "" >> MovieLensDataMetrics.txt
echo "Min / Max Nbr Ratings Per User" >> MovieLensDataMetrics.txt
hadoop fs -cat MinMaxNbrRatingsPerUserId/* >> MovieLensDataMetrics.txt
echo "" >> MovieLensDataMetrics.txt
echo "-----------------------------" >> MovieLensDataMetrics.txt
echo "" >> MovieLensDataMetrics.txt
echo "Avg Nbr Tags Per User" >> MovieLensDataMetrics.txt
hadoop fs -cat AvgNbrTagsPerUserId/* >> MovieLensDataMetrics.txt
echo "" >> MovieLensDataMetrics.txt
echo "-----------------------------" >> MovieLensDataMetrics.txt
echo "" >> MovieLensDataMetrics.txt
echo "Min / Max Nbr Tags Per User" >> MovieLensDataMetrics.txt
hadoop fs -cat MinMaxNbrTagsPerUserId/* >> MovieLensDataMetrics.txt
echo "" >> MovieLensDataMetrics.txt
echo "-----------------------------" >> MovieLensDataMetrics.txt
echo "" >> UserRatingActivityFreqDistribution.txt
echo "User Ratings Activity Frequency Distribution" >> UserRatingActivityFreqDistribution.txt
hadoop fs -cat UserRatingActivityFreqDistribution/* >> UserRatingActivityFreqDistribution.txt
echo "" >> UserRatingActivityFreqDistribution.txt
echo "-----------------------------" >> UserRatingActivityFreqDistribution.txt
echo "" >> UserTaggingActivityFreqDistribution.txt
echo "User Tagging Activity Frequency Distribution" >> UserTaggingActivityFreqDistribution.txt
hadoop fs -cat UserTaggingActivityFreqDistribution/* >> UserTaggingActivityFreqDistribution.txt
echo "" >> UserTaggingActivityFreqDistribution.txt
echo "-----------------------------" >> UserTaggingActivityFreqDistribution.txt

