package movielens;

import java.io.IOException;
import java.util.Collections;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Mapper Output:
 * 		list(K2, V2) = list(avgRating, title)	
 * 
 * Reducer Input:
 * 		(K2, list(V2)) = (avgRating, List(title))  
 * 
 * Reducer Output:
 * 		(K3, V3) = (avgRating, title)
 */ 

public class SortMovieTitlesByRatingReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
	
	ArrayList<String> titleList = new ArrayList<String>();
	Text rValue = new Text();
  
	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		titleList.clear();
		
		// Construct a list of titles for this rating.
		for (Text value : values) {		
			titleList.add(value.toString());
		}
		
		// Order the titles alphabetically if they have the same rating.
		Collections.sort(titleList);
		
		/*
		 * For each value, produce a key value pair for each title in the list.
		 */
		
		for (String title : titleList) {		
			rValue.set(title);
			context.write(key, rValue);
		}
		
	}
}