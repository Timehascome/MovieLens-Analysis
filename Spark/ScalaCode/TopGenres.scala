
package MovieLens

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object TopGenres {
    def main(args: Array[String]) {

        // Computes the top genres by average rating from the MovieLens dataset.
        //
        // Input Arguments:
        //     args(0) = movies.csv input file
        //     args(1) = ratings.csv input file
        //     args(2) = Top Genres output directory
      
        // Get a Spark context.
        // Must change master from "local" to "yarn-cluster" to run on the Cluster.
        val conf = new SparkConf().setMaster("local").setAppName("Top Genres")
        val sc   = new SparkContext(conf)
        
        // Get the movie ids and their ratings from the CSV file and put them in an RDD to prepare for join.
        val ratingHdr   = "userId,movieId,rating,timestamp"
        val movieIdRating = sc.textFile(args(1)).filter(_ != ratingHdr)
                              .map(_.split(",")).map(x => (x(1),x(2)))
                              .mapValues(_.toDouble) // Convert ratings to Double data type  
                              .mapValues(x => (x,1)) // Make new value a tuple of rating and 1
                              .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)) // Sum ratings and counts for each movieId      
        
        // Get the movie ids and genres from the CSV file and put them in an RDD to prepare for join.
        // Regular expression from http://vichargrave.com/java-csv-parser
        val movieHdr       = "movieId,title,genres"
        val movieIdGenres  = sc.textFile(args(0)).filter(_ != movieHdr)
                               .map(_.split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))"))
                               .map(x => (x(0),x(2))) // keep movieId & genres
                               .flatMapValues(x => x.split("\\|")) // Parse the pipe separated genres
                               .join(movieIdRating) // KV pair is (Id, Tuple(Genre, Tuple(Rating Sum, Rating Count)))
                               .values // Don't need the Id anymore so Genre becomes the Key
                               .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)) // Sum ratings and counts for each genre
                               .mapValues(x => (x._1 / x._2)) // Compute Avg
                               .map(x => x.swap) // Avg becomes Key and Genre becomes value
                               .sortByKey(false) // Sort by Avg Rating descending
                               .saveAsTextFile(args(2)) // Save the sorted movies
        
        // Shutdown Spark
        sc.stop()
    
        // Goodbye
        System.exit(0)
    }
}
