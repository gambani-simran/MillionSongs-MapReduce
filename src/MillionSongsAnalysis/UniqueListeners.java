package MillionSongsAnalysis;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//number of unique listeners
public class UniqueListeners extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Unique listeners count");
    job.setJarByClass(UniqueListeners.class);
    job.setMapperClass(UniqueListenersMapper.class);
    job.setReducerClass(UniqueListenersReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    Path inp = new Path(args[0]);
    Path out = new Path(args[1]);
    FileInputFormat.addInputPath(job, inp);
    FileOutputFormat.setOutputPath(job, out);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    return 0;
  }

  public static void main(String[] args) throws Exception
  {
    int res = ToolRunner.run(new Configuration(), new UniqueListeners(),args);
    System.exit(res);
  }
}

