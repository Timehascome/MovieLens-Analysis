package movielens;

import java.lang.StringBuffer;
import org.apache.hadoop.io.Text;

// MovieRatingsParser is modeled after the Parser class described in
// Chapter 6, page 154 of "Hadoop: The Definitive Guide 4th Edition" by Tom White.
public class CalculateMovieTitleRatingsParser {
	
	private String   movieTitle;
	private String   ratingInfo;
	
	private int      ratingCountThreshold;
	private Boolean  includeMovie;
	
	private Boolean      dataIsValid;
	private StringBuffer dataValidationMessage= new StringBuffer();
	
	CalculateMovieTitleRatingsParser (int countThreshold) {
		ratingCountThreshold = countThreshold;
	}
	
	// Getters
	public String getMovieTitle() {
	    return movieTitle;
	}
	
	public String getRatingInfo() {
		return ratingInfo;
	}
	
	public Boolean includeMovie() {
		return includeMovie;
	}
	
	public Boolean isValidData() {
		return dataIsValid;
	}
	
	public String getDataValidationMessage() {
		return dataValidationMessage.toString();
	}
	
	// Setters
	private void initialize() {
		setDataValidationMessage();
	}
	
	// Setters
	private void setIsValidData(Boolean bool) {
		dataIsValid = bool;
	}
	
	private void setDataValidationMessage() {
		dataValidationMessage.setLength(0);
		setDataValidationMessage("");
	}
	
	private void setDataValidationMessage (String msg) {
		dataValidationMessage.append(msg);
	}

	// Two versions of parse method.
	// Can accept either a Text or a String variable.
	public void parse(Text record) {
		parse(record.toString());
	}
	
	public void parse(String record) {
		initialize();
		
		try {
				// Split the string into values as delimited by the commas.
				String lineParts[] = record.split("\t");
				
				// Extract movie title and rating info.
				movieTitle = lineParts[0];
				ratingInfo = lineParts[2];
				
				// Extract the ratings count.
				String ratingParts[] = ratingInfo.split("\\|");
				setIsValidData(true);
				
				// Keep the movie if the rating count exceeds the threshold.
				if (ratingParts.length == 2) {
					Integer ratingCount = Integer.parseInt(ratingParts[1]);
					if (ratingCount > ratingCountThreshold) {		
						includeMovie = true;
					}
					else {
						includeMovie = false;
					}
				}
				else {
					setIsValidData(false);
					setDataValidationMessage("Missing ratingCount in record!");
				}
		}
		
		catch (RuntimeException ex){
			setIsValidData(false);
			setDataValidationMessage("Could not parse movie line!");
		}
	    
	} // parse
	
} // MovieRatingsParser
