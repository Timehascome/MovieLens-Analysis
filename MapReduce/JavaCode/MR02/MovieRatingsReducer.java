package movielens;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Mapper Output:
 * 		list(K2, V2) = list(movieId, rating)	
 * 
 * Reducer Input:
 * 		(K2, list(V2)) = (movieId, List(rating))  
 * 
 * Reducer Output:
 * 		(K3, V3) = (movieId, sumRatings|countRatings)
 */   

public class MovieRatingsReducer extends Reducer<Text, DoubleWritable, Text, Text> {
	
	// Define a movie counter.
	enum MovieCounter {
		NUM_MOVIES
	};
	
	Text rValue = new Text();
  
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
	  
		double ratingsSum = 0.0;
		int ratingsCount = 0;
		
		/*
		 * For each movieId:
		 */
		// Define a counter for bad records.
		for (DoubleWritable value : values) {
		  
		  /*
		   * Sum and count the movie ratings.
		   */
			ratingsSum += value.get();
			ratingsCount += 1;
		}
		
		/*
		 * Reducer output is the MovieID and its ratings total and number of ratings separated by a pipe character.
		 * This is to allow more flexibility in the downstream MR stages. 
		 */
		context.getCounter(MovieCounter.NUM_MOVIES).increment(1);
		rValue.set(String.valueOf(ratingsSum) + "|" + String.valueOf(ratingsCount));
		context.write(key, rValue);
	}
}