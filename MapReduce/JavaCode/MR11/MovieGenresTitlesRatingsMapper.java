package movielens;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper Input:
 *      Receives records from the files in the movietitlesgenresratingsout directory
 * 		(K1, V1) = (title \t genres \t sumRatings|countRatings)   
 * 
 * Mapper Output:
 * 		list(K2, V2) = list((genre, title), sumRatings|countRatings) where K1 is a TextPair of genre and movie
 */

public class MovieGenresTitlesRatingsMapper extends Mapper<Text, Text, TextPair, Text> {
  
	TextPair genreTitle = new TextPair();
	Text 	 title      = new Text();
	Text 	 genre      = new Text();
	Text 	 ratingInfo = new Text();
	
	// Number of movie ratings must exceed the threshold.
    int minRatingsThreshold = 2;
	
	// Define a record counter.
	static enum MovieCounter {
		EXCLUDE_MOVIE,
	};
	
	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// Extract the Genre, Title and ratings info.
		String[] lineInput = value.toString().split("\t");
		String[] genres = lineInput[1].split("\\|");
		String[] rating = lineInput[2].split("\\|");
		
		if (Integer.parseInt(rating[1]) > minRatingsThreshold) {
			for (int i = 0; i < genres.length; i++) {
			
				genre.set(genres[i]);
				title.set(lineInput[0]);
	
				// Populate the TextPair that becomes the composite key for the reducer.
				genreTitle.set(genre, title);
				
			    ratingInfo.set(lineInput[2]);
			
				context.write(genreTitle, ratingInfo);
			}
		}
		else {
			context.getCounter(MovieCounter.EXCLUDE_MOVIE).increment(1);
		}
	}
}