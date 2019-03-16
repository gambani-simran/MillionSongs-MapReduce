package StringsDemo;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
 
public class UserQuery extends Configured implements Tool{
      public int run(String[] args) throws Exception
      {
    	  
            //creating a JobConf object and assigning a job name for identification purposes
        Configuration conf = new Configuration();

    	conf.set("mapper.word", "chide");
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(UserQuery.class);
        job.setMapperClass(UserQueryMapper.class);
        job.setCombinerClass(UserQueryReducer.class);
        job.setReducerClass(UserQueryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
            return 0;
      }
     
      public static void main(String[] args) throws Exception
      {
            // this main function will call run method defined above.
    	  int res = ToolRunner.run(new Configuration(), new UserQuery(),args);
            System.exit(res);
      }
}