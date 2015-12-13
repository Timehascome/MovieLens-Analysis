package movielens;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper Input:
 * 		Receives records from the movies.csv file (Comma Separated Value format)
 * 		Each CSV record is of the form => movieId,title,genres   
 * 
 * Mapper Output:
 * 		list(K2, V2) = list(movieId, title \t genres)
 */

public class MovieTitlesGenresMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	// Define a record counter.
	static enum MovieCounter {
		NUM_MOVIES,
		HEADER_RECORD,
		BAD_RECORD
	};
	
	Text mKey = new Text();
	Text mValue = new Text();
  
	private MovieTitlesGenresParser parser = new MovieTitlesGenresParser();
  
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// Call the parser to get the information needed to compute the key value pairs.
		parser.parse(value);
		
		// Examine the rating value and increment the appropriate counter
		// so we compile a frequency distribution of rating values.
		if (parser.isValidData()) {
			// Increment movie counter.
			context.getCounter(MovieCounter.NUM_MOVIES).increment(1);
			
			mKey.set(parser.getMovieId());
			mValue.set(parser.getMovieTitle() + "\t" + parser.getMovieGenres());
			context.write(mKey, mValue);
		}
		else {
			if (parser.getDataValidationMessage().matches("Header Record")) {
				context.getCounter(MovieCounter.HEADER_RECORD).increment(1);
			}
			else {
				context.getCounter(MovieCounter.BAD_RECORD).increment(1);
				System.err.println(parser.getDataValidationMessage() + value);
				context.setStatus("Detected bad record");
			}
		}
	}
}