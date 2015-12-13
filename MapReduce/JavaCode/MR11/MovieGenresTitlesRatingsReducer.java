package movielens;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Mapper Output:
 * 		list(K2, V2) = list((genre, title), sumRatings|countRatings) where K2 is a TextPair of genre and title	
 * 
 * Reducer Input:
 * 		(K2, list(V2)) = ((genre, title), List(ratingsSum|ratingsCount))  
 * 
 * Reducer Output:
 * 		(K3, V3) = ((genre, title),avgRating)
 */  

public class MovieGenresTitlesRatingsReducer extends Reducer<TextPair, Text, TextPair, DoubleWritable> {
	
	DoubleWritable rValue = new DoubleWritable();
  
	@Override
	public void reduce(TextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		double ratingsSum = 0.0;
		int    ratingsCount = 0;
		
		for (Text value : values) {
			String[] ratingInfo = value.toString().split("\\|");
			ratingsSum += Double.parseDouble(ratingInfo[0]);
			ratingsCount += Integer.parseInt(ratingInfo[1]);
		}
		
		rValue.set(ratingsSum / ratingsCount);
		
		//
		context.write(key, rValue);

	}
}