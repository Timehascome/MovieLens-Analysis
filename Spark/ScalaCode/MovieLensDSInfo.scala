
package MovieLens

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object DataSetInfo {
    def main(args: Array[String]) {

        // Computes miscellaneous data set metrics for the MovieLens data set.
        //
        // Input Arguments:
        //     args(0) = movies.csv input file
        //     args(1) = ratings.csv input file
        //     args(2) = tags.csv input file
      
        // Get a Spark context.
        // Must change master from "local" to "yarn-cluster" to run on the Cluster.
        val conf = new SparkConf().setMaster("local").setAppName("Movie Lens Dataset Metrics")
        val sc   = new SparkContext(conf)

        // Regular expression from http://vichargrave.com/java-csv-parser
	      val csvSplitExpr = ",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))"

      	/* --------------------------------------------------------------------------------------------------------------       
      	   
      	  Read in contents of input files.
      
      	  COUNTS
      
      	*/
              
        // Get the contents of the movies.csv file.
        val movieHdr  = "movieId,title,genres"
        val moviesAll = sc.textFile(args(0)).filter(_ != movieHdr)
        val nbrMovies = moviesAll.count
      
      	// Get the contents of the ratings.csv file.
      	val ratingsHdr = "userId,movieId,rating,timestamp"
      	val ratingsAll = sc.textFile(args(1)).filter(_ != ratingsHdr)
      	val nbrRatings = ratingsAll.count
      
      	// Get the contents of the tags.csv file.
      	val tagsHdr = "userId,movieId,tag,timestamp"
      	val tagsAll = sc.textFile(args(2)).filter(_ != tagsHdr)
      	val nbrTags = tagsAll.count
     
        // Output these later with some others yet to be determined.
      
      	/* --------------------------------------------------------------------------------------------------------------       
      
      	   GENRES
      	   
      	   Determine two different metrics from the movieGenresRDD
           1) Count of movies by genre
      	   2) Frequency distribution of movie counts
      
      	*/

        // Get the genre movie information.
        // Split out each movieId and genre pair. Movies can be in multiple genres so a movieId can appear multiple times.
        val movieIdGenre = moviesAll.map(_.split(csvSplitExpr)).map(x => (x(0),x(2)))
                                    .flatMapValues(x => x.split("\\|")) // Split out the genres into movieId genre Key Value pairs

        // Count of movies by genres
        val movieCountByGenre = movieIdGenre.map(x => x.swap) // Genre becomes the key, movieId becomes the value
                                            .mapValues (x => 1) // Replace the movieID with a 1 for totaling purposes
                                            .reduceByKey((x,y) => x + y) // Total movie counts for each genre
                                            .sortByKey()
                                            .saveAsTextFile("GenreMovieCounts")

        // Frequency distribution of movies in multiple genres
      	val movieFreqDistPerGenre = movieIdGenre.mapValues(x => 1) // movieId is the Key, 1 is the Value
                                                .reduceByKey((x,y) => x + y) // Count genre occurrences of each movieId 
                                                .map(x => x.swap) // Genre movie count becomes Key, movieId becomes Value
                                                .mapValues(x => 1) // Replace the value so the genre movie count frequency can be calculated
                                                .reduceByKey((x,y) => x + y)
                                                .sortByKey()
      						                              .saveAsTextFile("GenreMovieFreqDistribution")
            
      	/* --------------------------------------------------------------------------------------------------------------       
      	  
      	  RATINGS 
      	   
      	  Determine different metrics from the ratings file
          1) Rating distribution frequency
      		2) UserId distribution frequency
      		3) Average number of ratings per UserId
      	  4) Max, Min number of ratings per UserId
      
      	*/
      
        // Rating distribution frequency, e.g. how many 5 star ratings are there, 4 start ratings, 3 star rating etc...
      	val ratingStars = ratingsAll.map(_.split(","))
                                    .map(x => (x(2),1)) // Assign count of 1 to each rating
                                    .reduceByKey((x,y) => (x + y)) // Compute frequency of each star rating
                                    .sortByKey(false)
      				                      .saveAsTextFile("RatingsFreqDistribution")
                                                       
      	// User rating activity distribution frequency
      	val ratingUserId = ratingsAll.map(_.split(","))
                                     .map(x => (x(0),1)) // Assign count of 1 to each occurrence of UserId
                                     .reduceByKey((x,y) => (x + y)) // Compute # ratings per UserId
                                     
        val ratingActivity = ratingUserId.map(x => x.swap) // UserID rating count becomes key, UserID becomes the value
                                         .mapValues(x => 1) // Overwrite the UserID with a 1
                                         .reduceByKey((x,y) => x + y) // Compute the frequencies of each rating
                                         .sortByKey(false) // Sort the frequencies in descending order
                                         .saveAsTextFile("UserRatingActivityFreqDistribution")
        
        // Count number UserIds with ratings
        val nbrUserIdsWithRatings = ratingUserId.count()

      	// Avg Nbr Ratings Per UserId
      	val avgNbrRatingsPerUserId = ratingUserId.values
                                                 .map(x => ("Average Nbr Ratings Per User", x))
                                                 .mapValues(x => (x, 1)) 
                                                 .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      	                                         .mapValues(x => (x._1 / x._2))
      							                             .saveAsTextFile("AvgNbrRatingsPerUserId")
      
      	// Min, Max Number Ratings Per UserId
        val minMaxRatingsPerUserId = ratingUserId.mapValues(x => (x, x))
                                                 .values
                                                 .map(x => ("Min/Max Nbr Ratings Per User", x))
      						                               .reduceByKey((x,y) => (Math.min(x._1,y._1), Math.max(x._2,y._2)))
      						                               .saveAsTextFile("MinMaxNbrRatingsPerUserId")

      	/* --------------------------------------------------------------------------------------------------------------       
      	  
      	  TAGS 
      	   
      	  Determine different metrics from the tags file
      		1) Average number of tags per UserId
      	  2) Max, Min number of tags per UserId
      
      	*/
      	
      	val tagUserId = tagsAll.map(_.split(csvSplitExpr))
                               .map(x => (x(0),1)) // Assign count of 1 to each occurrence of UserId
                               .reduceByKey((x,y) => (x + y)) // Compute # tags per UserId
                                    
        // User tagging activity distribution frequency                             
        val taggingActivity = tagUserId.map(x => x.swap) // UserID rating count becomes key, UserID becomes the value
                                       .mapValues(x => 1) // Overwrite the UserID with a 1
                                       .reduceByKey((x,y) => x + y) // Compute the frequencies of each rating
                                       .sortByKey(false) // Sort the frequencies in descending order
                                       .saveAsTextFile("UserTaggingActivityFreqDistribution")
        
        // Count number of UserIds with tags
        val nbrUserIdsWithTags = tagUserId.count()
      
      	// Avg Number Tags per UserId
      	val avgNbrTagsPerUserId = tagUserId.values
      						                         .map(x => ("Average Nbr Tags Per User", x))
      						                         .mapValues(x => (x, 1))
      						                         .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      						                         .mapValues(x => (x._1 / x._2))
      						                         .saveAsTextFile("AvgNbrTagsPerUserId")
      
      	// Min, Max Number Tags Per UserId
      	val minMaxTagsPerUserId = tagUserId.mapValues(x => (x, x))
                                           .values
                                           .map(x => ("Min/Max Nbr Tags Per User", x))
      						                         .reduceByKey((x,y) => (Math.min(x._1,y._1), Math.max(x._2,y._2)))
      						                         .saveAsTextFile("MinMaxNbrTagsPerUserId")	
        
        // Multiple data set record counts output here
        val DSCounts = (sc.parallelize(Array(("Nbr Movies", nbrMovies), 
                                             ("Nbr Ratings", nbrRatings), 
                                             ("Nbr Tags", nbrTags),
                                             ("Nbr UserIds with Ratings", nbrUserIdsWithRatings), 
                                             ("Nbr UserIds with Tags", nbrUserIdsWithTags))))
                          .saveAsTextFile("DSCounts")
        
        // Shutdown Spark
        sc.stop()
        
        // Goodbye
        System.exit(0)
    }
}
