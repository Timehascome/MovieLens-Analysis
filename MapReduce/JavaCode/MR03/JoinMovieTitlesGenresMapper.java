package movielens;

/*
 * Code is taken and adapted from the book:
 * "Hadoop - The Definitive Guide (4th Edition)" by Tom White
 * page 271 as part of the Reduce-Side Join implementation..
 * 
 */

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper Input:
 *      Receives records from the files in the movietitlesgenresout directory
 * 		(K1, V1) = (movieId, title \t genres)   
 * 
 * Mapper Output:
 * 		list(K2, V2) = list((movieId,0), title \t genres) where K2 is a TextPair of movieId and tag
 */

public class JoinMovieTitlesGenresMapper extends Mapper<Text, Text, TextPair, Text> {
  
	TextPair movieIdTag = new TextPair();
	Text tag = new Text();
	
	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// Ensure record with the Title appears first to the reducer
		tag.set("0");
		
		// Populate the TextPair that becomes the composite key for the reducer.
		movieIdTag.set(key, tag);
		
		context.write(movieIdTag, value);		
	}
}