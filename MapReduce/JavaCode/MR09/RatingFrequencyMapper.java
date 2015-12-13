package movielens;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper Input:
 * 		Receives records from the movieratingsbyuseridout file (KeyValueTextInput format)
 * 		(K1, V1) => (userId, ratingcount)   
 * 
 * Mapper Output:
 * 		list(K2, V2) = list(rating counts, 1)
 */

public class RatingFrequencyMapper extends Mapper<Text, Text, Text, IntWritable> {
  
	IntWritable mValue = new IntWritable(1);
	
	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// Value becomes the new key which is the rating count for a user.
		context.write(value, mValue);

	}
}