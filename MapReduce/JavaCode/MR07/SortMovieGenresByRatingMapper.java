package movielens;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper Input:
 * 		Receives records from the moviegenreratingsout file (KeyValueTextInput format)
 * 		(K1, V1) => (genre, rating)   
 * 
 * Mapper Output:
 * 		list(K2, V2) = list(avgRating, genre)
 */

public class SortMovieGenresByRatingMapper extends Mapper<Text, Text, DoubleWritable, Text> {
  
	DoubleWritable mValue = new DoubleWritable();
	
	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// TextInputOutputFormat so we must convert the value 
		// from Text to DoubleWritable before swapping the key and value.
		Double avgRating = Double.parseDouble(value.toString());
		mValue.set(avgRating);
		
		// Effectively swapped the key and value.
		context.write(mValue, key);

	}
}