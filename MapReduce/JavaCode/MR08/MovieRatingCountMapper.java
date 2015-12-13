package movielens;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper Input:
 * 		Receives records from the ratings.csv file (Comma Separated Value format)
 * 		Each CSV record is of the form => userId,movieId,rating,timestamp   
 * 
 * Mapper Output:
 * 		list(K2, V2) = list(UserId, 1)
 */

public class MovieRatingCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> { 
	
	// Define a record counter.
	enum RecordCounter {
		HEADER_RECORD,
		BAD_RECORD
	};

	/*
	 * The map method runs once for each line of text in the input file.
	 */
	
	Text mKey = new Text();
	IntWritable mValue = new IntWritable(1);
  
	private MovieRatingCountParser parser = new MovieRatingCountParser();
  
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// Call the parser to get the information needed to compute the key value pairs.
		parser.parse(value);
		
		// Output the UserId and a count or 1 for the rating.
		if (parser.isValidData()) {			
			mKey.set(parser.getUserId());
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