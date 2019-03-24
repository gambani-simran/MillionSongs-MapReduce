package MillionSongsAnalysis;

import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
 
public class ArtistYearReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

@Override
public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
		throws IOException {
	// TODO Auto-generated method stub
	int c = 0;
    while (values.hasNext()) {
      c = c + values.next().get();
    }
    output.collect(key, new IntWritable(c));
	
}
}