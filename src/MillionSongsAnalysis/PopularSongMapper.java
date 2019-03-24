package MillionSongsAnalysis;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
 
public class PopularSongMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
      private Text songId = new Text();
      private IntWritable count = new IntWritable();
     
      @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
      {
            String line = value.toString();
            String[] arr = line.split("	");            
            songId.set(arr[1]);
            count.set(Integer.parseInt(arr[2]));
            context.write(songId, count);
       }
}
