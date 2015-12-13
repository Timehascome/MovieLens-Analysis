package movielens;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper Input:
 * 		Receives records from the movieratingstitlesbygenreout file (Text Input format)
 * 		(K1, V1) => (byte offset, avgRating \t genre \t title)   
 * 
 * Mapper Output:
 * 		list(K2, V2) = list(avgRating, title)
 */

public class SortMoviesByRatingWithinGenreMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
	
	DoubleWritable mKey = new DoubleWritable();
	Text mValue = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] line = value.toString().split("\t");
		
		// The avgRating becomes the new key.
		Double rating = Double.parseDouble(line[0]);
		mKey.set(rating);
		
		// The movie title becomes the value.
		mValue.set(line[2]);
		
		// Effectively swapped the key and value.
		context.write(mKey, mValue);

	}
}