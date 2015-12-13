package movielens;

import java.lang.StringBuffer;
import org.apache.hadoop.io.Text;

// MovieRatingsParser is modeled after the Parser class described in
// Chapter 6, page 154 of "Hadoop: The Definitive Guide 4th Edition" by Tom White.
public class CalculateMovieGenreRatingsParser {
	
	private String[] movieGenres;
	private String   ratingInfo;
	
	private Boolean      dataIsValid;
	private StringBuffer dataValidationMessage= new StringBuffer();
	
	// Getters
	public String[] getMovieGenres() {
	    return movieGenres;
	}
	
	public String getRatingInfo() {
		return ratingInfo;
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
				
				// Extract movie genres and rating info.
				movieGenres = lineParts[1].split("\\|");
				ratingInfo  = lineParts[2];
				
				setIsValidData(true);
		}
		
		catch (RuntimeException ex){
			setIsValidData(false);
			setDataValidationMessage("Could not parse movie line!");
		}
	    
	} // parse
	
} // MovieRatingsParser
