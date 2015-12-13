  package movielens;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Mapper Output:
 * 		list(K2, V2) = list(User Rating Count, 1)	
 * 
 * Reducer Input:
 * 		(K2, list(V2)) = (User Rating Count, List(1's for each userId rating))  
 * 
 * Reducer Output:
 * 		(K3, V3) = (User Rating Count, frequency)
 */   

public class RatingFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	IntWritable rValue = new IntWritable();
  
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
	  
		int frequency = 0;
		
		/*
		 * For each rating count:
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