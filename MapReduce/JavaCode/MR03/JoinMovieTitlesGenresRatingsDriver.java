package movielens;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* 
 * The following is the code for the driver class.
 * 
 * Code is taken and adapted from the book:
 * "Hadoop - The Definitive Guide (4th Edition)" by Tom White
 * pages 272 and 273 as part of the Reduce-Side Join implementation..
 * 
 */

public class JoinMovieTitlesGenresRatingsDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			//JobBuilder.printUsage(this, "<titles input> <ratings input> <output>");
			return -1;
		}
		
    	//Instantiate a Job object for your job's configuration. 
    	Job job = new Job(getConf());
    
    	// Specify the jar file that contains the driver, mapper, and reducer.
    	// Hadoop will transfer this jar file to nodes in your cluster running 
    	// mapper and reducer tasks.
    	job.setJarByClass(JoinMovieTitlesGenresRatingsDriver.class);
    
    	// Specify an easily-decipherable name for the job.
    	// This job name will appear in reports and logs.
    	job.setJobName("Join Movie Titles Genres and Ratings");

    	// Specify the paths to the input and output data based on the
    	// command-line arguments.
    	Path movieTitlesPath  = new Path(args[0]);
    	Path movieRatingsPath = new Path(args[1]);
    	Path outputPath       = new Path(args[2]);
    	
    	MultipleInputs.addInputPath(job,  movieTitlesPath, KeyValueTextInputFormat.class, JoinMovieTitlesGenresMapper.class);
    	MultipleInputs.addInputPath(job,  movieRatingsPath, KeyValueTextInputFormat.class, JoinMovieRatingsMapper.class);
    	FileOutputFormat.setOutputPath(job,  outputPath);
    	
    	job.setPartitionerClass(JoinMovieTitlesGenresRatingsPartitioner.class);
    	job.setGroupingComparatorClass(FirstComparator.class);  	
    	
    	// Specify the reducer class.
        job.setReducerClass(JoinMovieTitlesGenresRatingsReducer.class);
        
        // Specify mapper output.
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(Text.class);
    
        // Specify reducer output.
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);

		//return (job.waitForCompletion(true) ? 0 : 1);
		boolean success = job.waitForCompletion(true);
		if (success) {
			Counters counters = job.getCounters();
		    
		    long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();		    	    
		    System.out.printf("\nTotal Records   : %,10d\n", total);
			
			return 0;
		} 
		else {
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new JoinMovieTitlesGenresRatingsDriver(), args);
		System.exit(exitCode);
	}
}