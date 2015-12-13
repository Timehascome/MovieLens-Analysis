package movielens;

/*
 * Code is taken and adapted from the book:
 * "Hadoop - The Definitive Guide (4th Edition)" by Tom White
 * pages 271 and 272 as part of the Reduce-Side Join implementation.
 * 
 */

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 *  Reducer receives two input files for the join.
 *  
 * Mapper Output:
 * 		list(K2, V2) = list((movieId,0), title \t genres) where K2 is a TextPair of movieId and tag 
 * 		list(K2, V2) = list((movieId,1), sumRatings|countRatings) where K2 is a TextPair of movieId and tag	
 * 
 * Reducer Input:
 * 		(K2, list(V2)) = ((movieId, 0), List(title \t genres))
 *      (K2, list(V2)) = ((movieId, 1), List(sumRatings|countRatings))  
 * 
 * Reducer Output:
 * 		(K3, V3) = (movieId, title \t genres \t sumRatings|countRatings)
 */ 

public class JoinMovieTitlesGenresRatingsReducer extends Reducer<TextPair, Text, Text, Text> {
	
	Text outValue = new Text();
  
	@Override
	public void reduce(TextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Iterator<Text> iter = values.iterator();
		// Please see Tom White's Scorpion comment on page 272
		// of "Hadoop - The Definitive Guide 4th Edition regarding
		// why a new Text object must be created here in
		// order to get the Title. He implies that this is a
		// Hadoop bug and I have verified that if the object
		// is created as new outside the reduce method, the
		// wrong value is retrieved.
		Text info = new Text(iter.next());
		while (iter.hasNext()) {
			Text ratings = iter.next();
			//Text outValue = new Text(title.toString() + "\t" + record.toString());
			outValue.set(info.toString() + "\t" + ratings.toString());
			context.write(key.getFirst(), outValue);
		}
	}
}