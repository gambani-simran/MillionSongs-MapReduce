package StringsDemo;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

 
public class UserQueryMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
      //hadoop supported data types
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();
     
      //map method that performs the tokenizer job and framing the initial key value pairs
      // after all lines are converted into key-value pairs, reducer is called.
      public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException
      {
            //taking one line at a time from input file and tokenizing the same
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            Configuration config = context.getConfiguration();
            String wordstring = config.get("mapper.word");
          //iterating through all the words available in that line and forming the key value pair
            while (tokenizer.hasMoreTokens())
            {
            	String token = tokenizer.nextToken();
            	if (wordstring.equals(token)) {
               //sending to output collector which inturn passes the same to reducer
			        word.set(token);
			        context.write(word, one);
            	}
            }
       }
}
