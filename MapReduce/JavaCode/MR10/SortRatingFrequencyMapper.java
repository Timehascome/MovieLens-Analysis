package movielens;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper Input:
 * 		Receives records from the movieratingsbyuseridout file (KeyValueTextInput format)
 * 		(K1, V1) => (userId, ratingCount)   
 * 
 * Mapper Output:
 * 		list(K2, V2) = list(ratingCount, 1)
 */

public class SortRatingFrequencyMapper extends Mapper<Text, Text, IntWritable, IntWritable> {
  
	IntWritable mKey   = new IntWritable();
	IntWritable mValue = new IntWritable();
	
	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		
		mKey.set(Integer.parseInt(key.toString()));
		mValue.set(Integer.parseInt(value.toString()));
		
		// Value becomes the new key which is the rating count for a user.
		context.write(mKey, mValue);

	}
}