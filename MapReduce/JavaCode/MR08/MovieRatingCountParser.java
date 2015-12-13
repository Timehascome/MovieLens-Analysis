package movielens;

import java.lang.StringBuffer;

import org.apache.hadoop.io.Text;

// MovieRatingsParser is modeled after the Parser class described in
// Chapter 6, page 154 of "Hadoop: The Definitive Guide 4th Edition" by Tom White.
public class MovieRatingCountParser {
	
	private String[]     lineParts;
	private String       userId;
	private String       ratingAsString;
	private Double       rating;
	private Boolean      dataIsValid;
	private StringBuffer dataValidationMessage= new StringBuffer();
	
	// Getters
	public String getUserId() {
		return userId;
	}
	
	public Double getRating() {
	    return rating;
	}
	
	public String getRatingAsString() {
		return ratingAsString;
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
			lineParts = record.split(",");
			userId = lineParts[0];
			String movie = lineParts[1];
			if (movie.matches("movieId")) {
				setIsValidData(false);
				setDataValidationMessage("Header Record");
			}
			else {
				ratingAsString = lineParts[2];
				rating = Double.parseDouble(lineParts[2]);
				setIsValidData(true);
			}
		}
		
		catch (RuntimeException ex){
			setIsValidData(false);
			setDataValidationMessage("Could not parse movie line!");
		}
	    
	} // parse
	
} // MovieRatingsParser
