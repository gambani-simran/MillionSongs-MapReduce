package MillionSongsAnalysis;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
 
public class EveryYearMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
{
      private Text year = new Text("");
      private final static IntWritable one = new IntWritable(1);
      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
      {
            String line = value.toString();
            String[] arr = line.split(",");
            year.set(arr[4]);
            output.collect(year, one);
       }
}
