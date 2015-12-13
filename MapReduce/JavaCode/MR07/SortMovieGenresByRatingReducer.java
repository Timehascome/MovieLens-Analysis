package movielens;

import java.io.IOException;
import java.util.Collections;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Mapper Output:
 * 		list(K2, V2) = list(avgRating, genre)	
 * 
 * Reducer Input:
 * 		(K2, list(V2)) = (avgRating, List(genre))  
 * 
 * Reducer Output:
 * 		(K3, V3) = (avgRating, genre)
 */ 

public class SortMovieGenresByRatingReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
	
	ArrayList<String> genreList = new ArrayList<String>();
	Text rValue = new Text();
  
	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		genreList.clear();
		
		// Construct a list of genres for this rating.
		for (Text value : values) {		
			genreList.add(value.toString());
		}
		
		// Order the genres alphabetically if they have the same rating.
		Collections.sort(genreList);
		
		/*
		 * For each value, produce a key value pair for each genre in the list.
		 */
		
		for (String genre : genreList) {		
			rValue.set(genre);
			context.write(key, rValue);
		}
		
	}
}