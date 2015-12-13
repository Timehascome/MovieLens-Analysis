package movielens;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Mapper Output:
 * 		list(K2, V2) = list(title, sumRatings|countRatings)	
 * 
 * Reducer Input:
 * 		(K2, list(V2)) = (title, List(sumRatings|countRatings))  
 * 
 * Reducer Output:
 * 		(K3, V3) = (title, avgRating)
 */ 

public class CalculateMovieTitleRatingsReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	 
	DoubleWritable rValue = new DoubleWritable();
  
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		/*
		 * For each value which is a pipe separated composite of a movieratingsum and a movieratingcount:
		 */
		Double  ratingsSum = 0.0;
		Integer ratingsCount  = 0;
		
		for (Text value : values) {
			// Extract the ratings count.
			String ratingParts[] = value.toString().split("\\|");
			
			ratingsSum   += Double.parseDouble(ratingParts[0]);
			ratingsCount += Integer.parseInt(ratingParts[1]);
		}
		
		/*
		 * Reducer output is the movie title and the rating.
		 * The output key is the same as the input key. 
		 */
		rValue.set(ratingsSum / ratingsCount);
		context.write(key, rValue);
	}
}