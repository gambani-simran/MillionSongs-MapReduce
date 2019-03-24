package MillionSongsAnalysis;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import java.util.Map.Entry;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class TopTen extends Configured implements Tool {
	
	 //Mappers
	 public static class TopTenMapper extends Mapper<LongWritable, Text, Text, Text>
	 {
	      private Text songId = new Text();
	      @Override
	      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	      {
	            //taking one line at a time from input file and tokenizing the same
	            String line = value.toString();
	            String[] arr = line.split("	");            
	            songId.set(arr[1]);
	            context.write(songId, new Text("count,"+arr[2]));
	       }
	}
	 
	 public static class InfoMapper extends Mapper<LongWritable, Text, Text, Text>
	 {
	      private Text sId = new Text();
	      @Override
	      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	      {
	            //taking one line at a time from input file and tokenizing the same
	            String line = value.toString();
	            String[] arr = line.split(",");            
	            sId.set(arr[0]);
	            context.write(sId, new Text("title,"+arr[1]));
	       }
	}
	 
	 //Reducer
	 public static class TopTenReducer extends Reducer <Text, Text, Text, Text>
	 {
		 HashMap<Text, IntWritable> playcountmap = new HashMap<Text, IntWritable>();
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		 {
			 String name = "";
			 int c = 0;
			 int x;
			 for (Text t : values) 
			 { 
			 String[] parts = t.toString().split(",");
			 if (parts[0].equals("count")) 
			 {
				 x = Integer.parseInt(parts[1]);
				 c = c + x;
			 } 
			 else if (parts[0].equals("title")) 
			 {
				 if(parts.length!=2) {
					 name = "NA";
				 }else {
					 name = parts[1];
				 }
			 }
			 }
			 
			 String str = String.format("%s\t%s", key.toString(), name);	//songId + title
			 if(c!=0) {
				 //String s = String.format("\t:\t%d", c);	//playcount
				 //context.write(new Text(str), new Text(s));
				 playcountmap.put(new Text(str), new IntWritable(c));
			 }
		 }
		 
		 @Override
		  public void cleanup(Context context)
		      throws IOException, InterruptedException {		
			Map<Text, IntWritable> sortedMap = sortByValues(playcountmap);
	        int counter = 0;
	        for (Text key : sortedMap.keySet()) {
	            if (counter++ == 20) {
	                break;
	            }
	            String s = "\t:\t"+sortedMap.get(key);
	            context.write(key, new Text(s));
	        }
			
		  }
		
		public static <K extends Comparable,V extends Comparable> Map<K,V> sortByValues(Map<K,V> map){
	        List<Map.Entry<K,V>> entries = new LinkedList<Map.Entry<K,V>>(map.entrySet());
	      
	        Collections.sort(entries, new Comparator<Map.Entry<K,V>>() {
	        	@Override
	            public int compare(Entry<K, V> o1, Entry<K, V> o2) {
	                return o2.getValue().compareTo(o1.getValue());	//descending
	            }
	        });
	        //LinkedHashMap will keep the keys in the order they are inserted which is currently sorted on natural ordering
	        Map<K,V> sortedMap = new LinkedHashMap<K,V>();
	        for(Map.Entry<K,V> entry: entries){
	            sortedMap.put(entry.getKey(), entry.getValue());
	        }
	        return sortedMap;
	    }
	 }
	
      //run job
	  public int run(String[] args) throws Exception {
		    Configuration conf = new Configuration();

		    Job job = Job.getInstance(conf, "Top ten using reduce-side join");
		    job.setJarByClass(TopTen.class);
		    //job.setMapperClass(TopTenMapper.class);
		    job.setReducerClass(TopTenReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, TopTenMapper.class);
		    MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, InfoMapper.class);
		    Path out = new Path(args[2]);
		    FileOutputFormat.setOutputPath(job, out);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		    return 0;
		  }

		  public static void main(String[] args) throws Exception
		  {
		        // this main function will call run method defined above.
			  	int res = ToolRunner.run(new Configuration(), new TopTen(),args);
		        System.exit(res);
		  }
		}
