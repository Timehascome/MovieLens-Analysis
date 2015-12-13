package movielens;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Excludes records with an insufficient number of ratings as specified by the input parameter.
 * 
 * Mapper Input:
 * 		Receives records from the moviestitlesgenresratingsout file (KeyValueTextInput format)
 * 		(K1, V1) => (movieId, title \t genres \t sumRatings|countRatings)   
 * 
 * Mapper Output:
 * 		list(K2, V2) = list(title, sumRatings|countRatings)
 */

public class CalculateMovieTitleRatingsMapper extends Mapper<Text, Text, Text, Text> {

	// Number of movie ratings must exceed the threshold.
	int minRatingsThreshold = 2;
	
	// Define a record counter.
	static enum MovieCounter {
		EXCLUDE_MOVIE,
		BAD_RECORD
	};
	
	Text mKey = new Text();
	Text mValue = new Text();
	
	// Instantiate parser with the minimum ratings threshold.
	private CalculateMovieTitleRatingsParser parser = new CalculateMovieTitleRatingsParser(minRatingsThreshold);
  
	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		// Call the parser to get the information needed to compute the key value pairs.
		parser.parse(value);
		
		// Only movies with a sufficient number of ratings are included.
		if (parser.isValidData() && parser.includeMovie()) {
			mKey.set(parser.getMovieTitle());
			mValue.set(parser.getRatingInfo());
			context.write(mKey, mValue);
		}
		else if (!parser.includeMovie()) {
			context.getCounter(MovieCounter.EXCLUDE_MOVIE).increment(1);
		}
		else {
			context.getCounter(MovieCounter.BAD_RECORD).increment(1);
			System.err.println(parser.getDataValidationMessage() + value);
			context.setStatus("Detected bad record");
		}
	}
}