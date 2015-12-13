package movielens;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Mapper Output:
 * 		list(K2, V2) = list((genre, title \t avgRating)	
 * 
 * Reducer Input:
 * 		(K2, list(V2)) = (genre, List(title, avgRating)  
 * 
 * Reducer Output:
 * 		(K3, V3) = (NullWritable, avgRating \t genre \t title) in separate files for each genre
 */  

public class MovieRatingsTitlesByGenreReducer extends Reducer<Text, Text, NullWritable, Text> {
  
	private MultipleOutputs<NullWritable, Text> multipleOutputs;
	
	@Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
    }
	
	Text rValue = new Text();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String genre;
		if (key.toString().contains("no genres listed")) {
			genre = "None";
		}
		else {
			genre = key.toString();
		}
		
		for (Text value : values) {
			// Get the title and the average rating.
			String line[] = value.toString().split("\t");
			
			// Form a new value ordered by the rating \t genre \t title.
			rValue.set(line[1] + "\t" + genre + "\t" + line[0]);
			
			// The key is the genre. It will form part of the file name.
			String basePath = String.format("%s/part", genre);
			multipleOutputs.write(NullWritable.get(), rValue, basePath);
		}
	}
	
    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
    	multipleOutputs.close();
    }
}