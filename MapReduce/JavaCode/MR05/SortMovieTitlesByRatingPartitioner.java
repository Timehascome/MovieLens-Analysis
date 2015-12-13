package movielens;

/*
 * Code is taken and adapted from the book:
 * "Hadoop - The Definitive Guide (4th Edition)" by Tom White
 * page 272.
 * 
 */

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * This is the class to specify where the Reducer output goes as part of the Reduce-Side join.
 */ 
public class SortMovieTitlesByRatingPartitioner extends Partitioner<DoubleWritable, Text> {
	
	@Override
	public int getPartition (DoubleWritable key, Text value, int numPartitions) {

			double keyValue = key.get();
			
			if (keyValue > 4.5) {
				return 0;
			}
			else if (keyValue > 4.0) {
				return 1;
			}
			else if (keyValue > 3.5) {
				return 2;
			}
			else if (keyValue > 3.0) {
				return 3;
			}
			else if (keyValue > 2.5) {
				return 4;
			}
			else return 5;
		
		}

}