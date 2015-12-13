
package MovieLens

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner

/*
 * Custom partitioner modeled after DomainNamePartitioner
 * on page 69 of "Learning Spark" by Holden Karau, et. al., O-Reilly Media Inc.
 */
class GenrePartitioner(nbrGenres: Int) extends Partitioner {
    override def numPartitions: Int = nbrGenres
    override def getPartition(key: Any): Int = {
      
        val genre = key.toString
        
        if      (genre == "Action")      {0}
        else if (genre == "Adventure")   {1}
        else if (genre == "Animation")   {2}
        else if (genre == "Children")    {3}
        else if (genre == "Comedy")      {4}
        else if (genre == "Crime")       {5}
        else if (genre == "Documentary") {6}
        else if (genre == "Drama")       {7}
        else if (genre == "Fantasy")     {8}
        else if (genre == "Film-Noir")   {9}
        else if (genre == "Horror")      {10}
        else if (genre == "IMAX")        {11}
        else if (genre == "Musical")     {12}
        else if (genre == "Mystery")     {13}
        else if (genre == "Romance")     {14}
        else if (genre == "Sci-Fi")      {15}
        else if (genre == "Thriller")    {16}
        else if (genre == "War")         {17}
        else if (genre == "Western")     {18}
        else                             {19} 
    }

    // Java equals method to let Spark compare our Partitioner objects
    override def equals(other: Any): Boolean = other match {
        case gp: GenrePartitioner => gp.numPartitions == numPartitions
        case _ => false
    }
}

object TopMoviesInGenres {
    def main(args: Array[String]) {

        // Computes the top movies by average rating from the MovieLens dataset.
        //
        // Input Arguments:
        //     args(0) = Minimum number of ratings a movie must have received to be included in the total.
        //     args(1) = movies.csv input file
        //     args(2) = ratings.csv input file
        //     args(3) = Top Movies In Genres output directory

        val nbrGenres = 20
      
        // Get a Spark context.
        // Must change master from "local" to "yarn-cluster" to run on the Cluster.
        val conf = new SparkConf().setMaster("yarn-cluster").setAppName("Top Movies In Genres")
        val sc   = new SparkContext(conf)

        // Regular expression from http://vichargrave.com/java-csv-parser
        val csvSplitExpr = ",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))"
        
        // Make the min threshhold a number instead of a string.
        val minRatingsRequired = args(0).toInt;
        
        // Get the movie ids and titles from the CSV file and put them in an RDD to prepare for join.
        // Regular expression from http://vichargrave.com/java-csv-parser
        val movieHdr      = "movieId,title,genres"
        val movieIdTitle  = sc.textFile(args(1)).filter(_ != movieHdr)
                              .map(_.split(csvSplitExpr))
                              .map(x => (x(0),x(1))) // keep movieId & title

        // Get the movie ids and genres from the CSV file and put them in an RDD to prepare for join.
        // Regular expression from http://vichargrave.com/java-csv-parser
        val movieIdGenre  = sc.textFile(args(1)).filter(_ != movieHdr)
                              .map(_.split(csvSplitExpr))
                              .map(x => (x(0),x(2)))//.collect() // keep movieId & genres
                              .flatMapValues(x => x.split("\\|")) // Parse the pipe separated genres. (K,V) is (movieId, genre)
        
        // Get the movie ids and their ratings from the CSV file and put them in an RDD to prepare for join.
        val ratingHdr     = "userId,movieId,rating,timestamp"
        val movieIdRating = sc.textFile(args(2)).filter(_ != ratingHdr)
                              .map(_.split(",")).map(x => (x(1),x(2))) // KV is (movieId, rating)

        // Compute average rating for each movie id and add the title.
        val movieIdAvgRatingTitle = movieIdRating.mapValues(_.toDouble) // Convert ratings to Double datatype  
                                                 .mapValues(x => (x,1)) // Make new value a tuple of rating and 1
                                                 .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)) // Sum ratings and counts
                                                 .mapValues(x => if (x._2 > minRatingsRequired) {(x._1 / x._2)} else {0.0}) // Compute Avg Rating
                                                 .join(movieIdTitle) // (K,V) is (movieId, (avgRating, Title))
 
        // Add the average ratings and titles to the genres.
        // mapPartitions() iterates over the values in the RDD partition and sorts the movies in each genre partition by the average rating.
        // mapPartitions() call adapted from example at:
        //     http://apache-spark-developers-list.1001551.n3.nabble.com/Sorting-partitions-in-Java-td6715.html
        val genreRatingTitle = movieIdGenre.join(movieIdAvgRatingTitle)
                                           .values // Lose the movieId, KV becomes (Genre, Tuple(avgRating, Title))
                                           .partitionBy(new GenrePartitioner(nbrGenres)) // Each genre's movies go to their own genre partitioner
                                           .mapPartitions(iter => {iter.toArray.sortWith((x,y) => x._2._1.compare(y._2._1)>0).iterator},preservesPartitioning=true)
                                           .saveAsTextFile(args(3))
                                           
        // Shutdown Spark
        sc.stop()
    
        // Exit
        System.exit(0)
    }
}
