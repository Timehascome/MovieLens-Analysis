package movielens;

import movielens.MovieRatingsMapper.RatingValues;
import movielens.MovieRatingsMapper.RecordCounter;
import movielens.MovieRatingsReducer.MovieCounter;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/* 
 * The following is the code for the driver class:
 */
public class MovieRatingsDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
    	//Instantiate a Job object for your job's configuration. 
    	Job job = new Job(getConf());
    
    	// Specify the jar file that contains the driver, mapper, and reducer.
    	// Hadoop will transfer this jar file to nodes in your cluster running 
    	// mapper and reducer tasks.
    	job.setJarByClass(MovieRatingsDriver.class);
    
    	// Specify an easily-decipherable name for the job.
    	// This job name will appear in reports and logs.
    	job.setJobName("Movie Ratings by ID");

    	// Specify the paths to the input and output data based on the
    	// command-line arguments.
    	FileInputFormat.setInputPaths(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	
    	// Specify the mapper and reducer classes.
        job.setMapperClass(MovieRatingsMapper.class);
        job.setReducerClass(MovieRatingsReducer.class);
        
        // Specify mapper output.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
    
        // Specify reducer output.
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);

		//return (job.waitForCompletion(true) ? 0 : 1);
		boolean success = job.waitForCompletion(true);
		if (success) {
		    Counters counters = job.getCounters();
		    long ctr5Stars = counters.findCounter(RatingValues.FIVE_STARS).getValue();
		    long ctr4pt5Stars = counters.findCounter(RatingValues.FOUR_AND_ONE_HALF_STARS).getValue();
		    long ctr4Stars = counters.findCounter(RatingValues.FOUR_STARS).getValue();
		    long ctr3pt5Stars = counters.findCounter(RatingValues.THREE_AND_ONE_HALF_STARS).getValue();
		    long ctr3Stars = counters.findCounter(RatingValues.THREE_STARS).getValue();
		    long ctr2pt5Stars = counters.findCounter(RatingValues.TWO_AND_ONE_HALF_STARS).getValue();
		    long ctr2Stars = counters.findCounter(RatingValues.TWO_STARS).getValue();
		    long ctr1pt5Stars = counters.findCounter(RatingValues.ONE_AND_ONE_HALF_STARS).getValue();
		    long ctr1Star = counters.findCounter(RatingValues.ONE_STAR).getValue();
		    long ctrHalfStar = counters.findCounter(RatingValues.ONE_HALF_STAR).getValue();
		    long ctr0Stars = counters.findCounter(RatingValues.ZERO_STARS).getValue();
		    
		    long ctrRatingErrors = counters.findCounter(RatingValues.RATING_ERROR).getValue();
		    
		    long total            = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
		    long ctrMovies        = counters.findCounter(MovieCounter.NUM_MOVIES).getValue();
		    long ctrHeaderRecords = counters.findCounter(RecordCounter.HEADER_RECORD).getValue();
		    long ctrBadRecords    = counters.findCounter(RecordCounter.BAD_RECORD).getValue();
		   
		    System.out.printf("Number movies receiving 5   Stars: %,10d\n", ctr5Stars);
		    System.out.printf("Number movies receiving 4.5 Stars: %,10d\n", ctr4pt5Stars);
		    System.out.printf("Number movies receiving 4   Stars: %,10d\n", ctr4Stars);
		    System.out.printf("Number movies receiving 3.5 Stars: %,10d\n", ctr3pt5Stars);
		    System.out.printf("Number movies receiving 3   Stars: %,10d\n", ctr3Stars);
		    System.out.printf("Number movies receiving 2.5 Stars: %,10d\n", ctr2pt5Stars);
		    System.out.printf("Number movies receiving 2   Stars: %,10d\n", ctr2Stars);
		    System.out.printf("Number movies receiving 1.5 Stars: %,10d\n", ctr1pt5Stars);
		    System.out.printf("Number movies receiving 1   Stars: %,10d\n", ctr1Star);
		    System.out.printf("Number movies receiving 0.5 Stars: %,10d\n", ctrHalfStar);
		    System.out.printf("Number movies receiving 0   Stars: %,10d\n", ctr0Stars);
		    System.out.printf("Rating Errors                    : %,10d\n", ctrRatingErrors);
		    		    	    
		    System.out.printf("\nTotal Records   : %,10d\n", total);
		    System.out.printf("Total Ratings   : %,10d\n", total);
		    System.out.printf("Total Movies    : %,10d\n", ctrMovies);
		    System.out.printf("Header Records  : %,10d\n", ctrHeaderRecords);
		    System.out.printf("Bad Records     : %,10d\n", ctrBadRecords);
			
			return 0;
		} 
		else {
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MovieRatingsDriver(), args);
		System.exit(exitCode);
	}
}