package movielens;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Mapper Output:
 * 		list(K2, V2) = list(ratingCount, 1)	
 * 
 * Reducer Input:
 * 		(K2, list(V2)) = (ratingCount, List(1's for each rating))  
 * 
 * Reducer Output:
 * 		(K3, V3) = (ratingCount, frequency)
 */   

public class SortRatingFrequencyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
	IntWritable rValue = new IntWritable();
  
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
	  
		int frequency = 0;
		
		/*
		 * For each Rating Count, count the frequency of its occurrence.
		 */
		for (IntWritable value : values) {
		  
		  /*
		   * Count the movie ratings.
		   */
			frequency += value.get();
		}
		
		/*
		 * Reducer output is the rating count and frequency.
		 */
		rValue.set(frequency);
		context.write(key, rValue);
	}
}