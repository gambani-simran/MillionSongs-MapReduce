package MillionSongsAnalysis;

import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
 
public class EveryYearReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
  
@Override
public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
		throws IOException {
	// TODO Auto-generated method stub
	int c = 0;
    while (values.hasNext()) {
      c = c + values.next().get();
    }
    int z = Integer.parseInt(key.toString());
    if(z!=0) {	//not considering unknown years
    	output.collect(key, new IntWritable(c));
    }
	
}
}