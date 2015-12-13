package movielens;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper Input:
 * 		Receives records from the moviestitlesgenresratingsout file (KeyValueTextInput format)
 * 		(K1, V1) => (movieId, title \t genres \t sumRatings|countRatings)   
 * 
 * Mapper Output:
 * 		list(K2, V2) = list(genre, sumRatings|countRatings)
 */

public class CalculateMovieGenreRatingsMapper extends Mapper<Text, Text, Text, Text> {
	
	// Define a record counter.
	static enum MovieCounter {
		BAD_RECORD
	};
	
	Text mKey = new Text();
	Text mValue = new Text();
	
	private CalculateMovieGenreRatingsParser parser = new CalculateMovieGenreRatingsParser();
  
	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		// Call the parser to get the information needed to compute the key value pairs.
		parser.parse(value);
		
		// Only movies with a sufficient number of ratings are included.
		if (parser.isValidData()) {
			for (int i = 0; i < parser.getMovieGenres().length; i++) {
				mKey.set(parser.getMovieGenres()[i]);
				mValue.set(parser.getRatingInfo());
				context.write(mKey, mValue);
			}
		}
		else {
			context.getCounter(MovieCounter.BAD_RECORD).increment(1);
			System.err.println(parser.getDataValidationMessage() + value);
			context.setStatus("Detected bad record");
		}
	}
}