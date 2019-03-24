package MillionSongsAnalysis;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
 
public class UniqueListenersMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
      private Text userId = new Text();
      private IntWritable one = new IntWritable(1);
      @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
      {
            String line = value.toString();
            String[] arr = line.split("	");            
            userId.set(arr[0]);
            context.write(userId, one);
       }
}
