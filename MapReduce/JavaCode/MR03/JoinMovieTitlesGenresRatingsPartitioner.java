package movielens;

/*
 * Code is taken and adapted from the book:
 * "Hadoop - The Definitive Guide (4th Edition)" by Tom White
 * page 272.
 * 
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * This is the class to specify where the Reducer output goes as part of the Reduce-Side join.
 */ 
public class JoinMovieTitlesGenresRatingsPartitioner extends Partitioner<TextPair, Text> {
	
	@Override
	public int getPartition (TextPair key, Text value, int numPartitions) {

			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		
		}

}