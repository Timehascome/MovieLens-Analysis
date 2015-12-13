package movielens;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper Input:
 *      Receives records from the files in the moviegenrestitlesratingsout directory
 * 		(K1, V1) = (genre, title \t avgRating)   
 * 
 * Mapper Output:
 * 		list(K2, V2) = list((genre, title \t avgRating)
 */

public class MovieRatingsTitlesByGenreMapper extends Mapper<Text, Text, Text, Text> {
	
	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		
		context.write(key, value);
	}
}