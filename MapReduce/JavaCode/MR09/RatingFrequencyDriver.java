package movielens;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* 
 * The following is the code for the driver class:
 */
public class RatingFrequencyDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
    	//Instantiate a Job object for your job's configuration. 
    	Job job = new Job(getConf());
    
    	// Specify the jar file that contains the driver, mapper, and reducer.
    	// Hadoop will transfer this jar file to nodes in your cluster running 
    	// mapper and reducer tasks.
    	job.setJarByClass(RatingFrequencyDriver.class);
    
    	// Specify an easily-decipherable name for the job.
    	// This job name will appear in reports and logs.
    	job.setJobName("Movie Rating Frequency");

    	// Specify the paths to the input and output data based on the
    	// command-line arguments.
    	FileInputFormat.setInputPaths(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	
    	// Specify the mapper, combiner, reducer and comparatorclasses.
        job.setMapperClass(RatingFrequencyMapper.class);
        job.setReducerClass(RatingFrequencyReducer.class);
        job.setCombinerClass(RatingFrequencyReducer.class);
        
     	// Input format.
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
        
        // Specify mapper output.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
    
        // Specify reducer output.
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);

		//return (job.waitForCompletion(true) ? 0 : 1);
		boolean success = job.waitForCompletion(true);
		if (success) {
		    Counters counters = job.getCounters();
		    
		    long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue(); 
		    
		    System.out.printf("\nTotal Records : %,10d\n", total);
			
			return 0;
		} 
		else {
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new RatingFrequencyDriver(), args);
		System.exit(exitCode);
	}
}