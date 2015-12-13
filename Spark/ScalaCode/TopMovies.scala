package MovieLens

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object TopMovies {
    def main(args: Array[String]) {

        // Computes the top movies by average rating from the MovieLens dataset.
        //
        // Input Arguments:
        //     args(0) = Minimum number of ratings a movie must have received to be included in the total.
        //     args(1) = movies.csv input file
        //     args(2) = ratings.csv input file
        //     args(3) = Top Movies output directory
      
        // Get a Spark context.
        // Must change master from "local" to "yarn-cluster" to run on the Cluster.
        val conf = new SparkConf().setMaster("local").setAppName("Top Movies")
        val sc   = new SparkContext(conf)
        
        // Make the Min threshhold a number instead of a string.
        val minRatingsRequired = args(0).toInt;
        
        // Get the movie ids and titles from the CSV file and put them in an RDD to prepare for join.
        // Regular expression from http://vichargrave.com/java-csv-parser
        val movieHdr      = "movieId,title,genres"
        val movieIdTitle  = sc.textFile(args(1)).filter(_ != movieHdr)
                             .map(_.split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))"))
                             .map(x => (x(0),x(1))) // keep movieId & title
        
        // Get the movie ids and their ratings from the CSV file and put them in an RDD to prepare for join.
        val ratingHdr     = "userId,movieId,rating,timestamp"
        val movieIdRating = sc.textFile(args(2)).filter(_ != ratingHdr)
                              .map(_.split(",")).map(x => (x(1),x(2)))
                              .mapValues(_.toDouble) // Convert ratings to Double datatype  
                              .mapValues(x => (x,1)) // Make new value a tuple of rating and 1
                              .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)) // Sum ratings and counts
                              .mapValues(x => if (x._2 > minRatingsRequired) {(x._1 / x._2)} else {0.0}) // Compute AVG
                              .join(movieIdTitle) // After join, Key = movieId; Value = Tuple (Avg Rating, Title)
                              .values // Keep only the values to form new RDD with KV pair of (Avg Rating, Title)
                              .sortByKey(false) // Sort by Avg Rating descending
                              .saveAsTextFile(args(3)) // Save the sorted movies
        
        // Shutdown Spark
        sc.stop()
        
        // Goodbye
        System.exit(0)
    }
}
