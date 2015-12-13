package movielens;

import java.lang.StringBuffer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

// MovieRatingsParser is modeled after the Parser class described in
// Chapter 6, page 154 of "Hadoop: The Definitive Guide 4th Edition" by Tom White.
public class MovieTitlesGenresParser {
	
	private String[]     lineParts;
	private String       movieId;
	private String       movieTitle;
	private String       movieGenres;
	private Boolean      dataIsValid;
	private StringBuffer dataValidationMessage= new StringBuffer();
	
	// Getters
	public String getMovieId() {
		return movieId;
	}
	
	public String getMovieTitle() {
	    return movieTitle;
	}
	
	public String getMovieGenres() {
		return movieGenres;
		
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
			// Detect double quotes. The Title could contain a comma.
			if (record.contains("\"")) {
				int quote1Index = StringUtils.indexOf(record, "\"");
				int quote2Index = StringUtils.lastIndexOf(record, "\"");
				lineParts[0] = record.substring(0, quote1Index - 1);
				lineParts[1] = record.substring(quote1Index + 1, quote2Index);
				lineParts[2] = record.substring(quote2Index+2, record.length());
			}
			else {
				// Split the string into values as delimited by the commas.
				lineParts = record.split(",");
			}
						
			movieId = lineParts[0];
			if (movieId.matches("movieId")) {
				setIsValidData(false);
				setDataValidationMessage("Header Record");
			}
			else {
				movieTitle = lineParts[1];
				movieGenres = lineParts[2];
				setIsValidData(true);
			}
		}
		
		catch (RuntimeException ex){
			setIsValidData(false);
			setDataValidationMessage("Could not parse movie line!");
		}
	    
	} // parse
	
} // MovieRatingsParser
