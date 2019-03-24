package MillionSongsAnalysis;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
 
public class ArtistYearMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
{
      //hadoop supported data types
      private Text aykey = new Text();
      private final static IntWritable one = new IntWritable(1);

      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
      {
            String line = value.toString();
            String[] arr = line.split(",");
            int z = Integer.parseInt(arr[4]);
            if(z==0) {
            	aykey.set(arr[3]+" in year not known: ");
            }else {
            	aykey.set(arr[3]+" in "+arr[4]+": ");
            }
            output.collect(aykey, one);
       }
}
