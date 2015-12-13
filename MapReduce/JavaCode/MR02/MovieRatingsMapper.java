package movielens;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper Input:
 * 		Receives records from the ratings.csv file (Comma Separated Value format)
 * 		Each CSV record is of the form => userId,movieId,rating,timestamp   
 * 
 * Mapper Output:
 * 		list(K2, V2) = list(movieId, rating)
 */

public class MovieRatingsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> { 
	
	// Define the counters so we can collect a frequency distribution of ratings.
	enum RatingValues {
		FIVE_STARS,
		FOUR_AND_ONE_HALF_STARS,
		FOUR_STARS,
		THREE_AND_ONE_HALF_STARS,
		THREE_STARS,
		TWO_AND_ONE_HALF_STARS,
		TWO_STARS,
		ONE_AND_ONE_HALF_STARS,
		ONE_STAR,
		ONE_HALF_STAR,
		ZERO_STARS,
		RATING_ERROR
	};
	
	// Define a record counter.
	enum RecordCounter {
		HEADER_RECORD,
		BAD_RECORD
	};

	/*
	 * The map method runs once for each line of text in the input file.
	 */
	
	Text mKey = new Text();
	DoubleWritable mValue = new DoubleWritable();
  
	private MovieRatingsParser parser = new MovieRatingsParser();
  
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// Call the parser to get the information needed to compute the key value pairs.
		parser.parse(value);
		
		// Examine the rating value and increment the appropriate counter
		// so we compile a frequency distribution of rating values.
		if (parser.isValidData()) {
			String movieRating = parser.getRatingAsString();
			if (movieRating.contains("5.0")) {
				context.getCounter(RatingValues.FIVE_STARS).increment(1);
			}
			else if (movieRating.contains("4.5")) {
				context.getCounter(RatingValues.FOUR_AND_ONE_HALF_STARS).increment(1);
			}
			else if (movieRating.contains("4.0")) {
				context.getCounter(RatingValues.FOUR_STARS).increment(1);
			}
			else if (movieRating.contains("3.5")) {
				context.getCounter(RatingValues.THREE_AND_ONE_HALF_STARS).increment(1);
			}
			else if (movieRating.contains("3.0")) {
				context.getCounter(RatingValues.THREE_STARS).increment(1);
			}
			else if (movieRating.contains("2.5")) {
				context.getCounter(RatingValues.TWO_AND_ONE_HALF_STARS).increment(1);
			}
			else if (movieRating.contains("2.0")) {
				context.getCounter(RatingValues.TWO_STARS).increment(1);
			}
			else if (movieRating.contains("1.5")) {
				context.getCounter(RatingValues.ONE_AND_ONE_HALF_STARS).increment(1);
			}
			else if (movieRating.contains("1.0")) {
				context.getCounter(RatingValues.ONE_STAR).increment(1);
			}
			else if (movieRating.contains(".5")) {
				context.getCounter(RatingValues.ONE_HALF_STAR).increment(1);
			}
			else if (movieRating.contains(".0")) {
				context.getCounter(RatingValues.ZERO_STARS).increment(1);
			}
			else {
				context.getCounter(RatingValues.RATING_ERROR).increment(1);
			}
			
			mKey.set(parser.getMovie());
			mValue.set(parser.getRating());
			context.write(mKey, mValue);
		}
		else {
			if (parser.getDataValidationMessage().matches("Header Record")) {
				context.getCounter(RecordCounter.HEADER_RECORD).increment(1);
			}
			else {
				context.getCounter(RecordCounter.BAD_RECORD).increment(1);
				System.err.println(parser.getDataValidationMessage() + value);
				context.setStatus("Detected bad record");

			}
		}
	}
}