package MillionSongsAnalysis;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
 
public class UniqueListenersReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private static int count = 0;
		
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException {
	    count = count + 1;
	}	
	@Override	//example of how to use cleanup OR close
	  public void cleanup(Context context)
	      throws IOException, InterruptedException {
            context.write(new Text("Number of unique listeners"), new IntWritable(count));
		
	  }

}
