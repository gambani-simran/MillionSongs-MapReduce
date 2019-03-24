package MillionSongsAnalysis;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import java.util.Map.Entry;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarUsers extends Configured implements Tool {
	 //Mapper
	 public static class SimilarUsersMapper extends Mapper<LongWritable, Text, Text, Text>
	 {
	      private Text userId = new Text();
	      private Text songId = new Text();
	      @Override
	      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	      {
	            String line = value.toString();
	            String[] arr = line.split("	");            
	            userId.set(arr[0]);
	            songId.set(arr[1]);
	            context.write(userId, songId);
	       }
	}
	 
	 //Reducer
	 public static class SimilarUsersReducer extends Reducer <Text, Text, Text, Text>
	 {
		 //for a map of String,ArrayList<String> i.e. of userId, list of songs listened to
		 Map<String, List<String>> usersongsmap = new HashMap<String, List<String>>();
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		 {	
			List<String> al1 = new ArrayList<String>();
 			for(Text v : values) {
				al1.add(v.toString());
			}
			usersongsmap.put(key.toString(), al1);
		 }
		 
		 @Override
		  public void cleanup(Context context)
		      throws IOException, InterruptedException {	
			 List<String> al2 = new ArrayList<String>();	//song list of every other user
			 List<String> u1al2 = new ArrayList<String>();	//song list of first user
			 //first user info
			 Map.Entry<String, List<String>> entry = usersongsmap.entrySet().iterator().next();
			 //String u1 = entry.getKey();
			 u1al2 = entry.getValue();
			 Map<Text, IntWritable> simMap = new HashMap<Text, IntWritable>();
			 for (Map.Entry<String, List<String>> e : usersongsmap.entrySet()) {
				    String key = e.getKey();
				    al2 = e.getValue();
				    al2.retainAll(u1al2);	//compare every other user with first user
				    //System.out.println(al2.size());
				    if(al2.size()!=0) {
				    	simMap.put(new Text(key),new IntWritable(al2.size()));
				    }				    
			}

	        for (Text key : simMap.keySet()) {
	            String s = "\t:\t"+simMap.get(key);
	            context.write(key, new Text(s));
	        }
			
		  }
	 }
	
      //run job
	  public int run(String[] args) throws Exception {
		    Configuration conf = new Configuration();

		    Job job = Job.getInstance(conf, "Similarity");
		    job.setJarByClass(SimilarUsers.class);
		    job.setMapperClass(SimilarUsersMapper.class);
		    job.setReducerClass(SimilarUsersReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    Path inp = new Path(args[0]);
		    Path out = new Path(args[1]);
		    FileInputFormat.addInputPath(job, inp);
		    FileOutputFormat.setOutputPath(job, out);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		    return 0;
		  }

		  public static void main(String[] args) throws Exception
		  {
		        // this main function will call run method defined above.
			  	int res = ToolRunner.run(new Configuration(), new SimilarUsers(),args);
		        System.exit(res);
		  }
	}
