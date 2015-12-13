package movielens;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Mapper Output:
 * 		list(K2, V2) = list(userId, 1)	
 * 
 * Reducer Input:
 * 		(K2, list(V2)) = (userId, List(1s for each userId rating))  
 * 
 * Reducer Output:
 * 		(K3, V3) = (userId, ratings count)
 */   

public class MovieRatingCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	IntWritable rValue = new IntWritable();
	
	enum UserIdCounter {
		NUM_USERS
	};
  
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
	  
		int ratingsCount = 0;
		
		/*
		 * For each movieId:
		 */
		// Define a counter for bad records.
		for (IntWritable value : values) {
		  
		  /*
		   * Count the movie ratings.
		   */
			ratingsCount += value.get();
		}
		
		/*
		 * Reducer output is the userID and number of ratings.
		 */
		context.getCounter(UserIdCounter.NUM_USERS).increment(1);
		rValue.set(ratingsCount);
		context.write(key, rValue);
	}
}