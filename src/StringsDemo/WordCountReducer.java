package StringsDemo;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
 
public class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
{
	private int maxcount = 0;
    private Text maxword = new Text("");
    OutputCollector<Text, IntWritable> op;

	//reduce method accepts the Key Value pairs from mappers, do the aggregation based on keys and produce the final out put
      public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
      {
            int sum = 0;
            /*iterates through all the values available with a key and add them together and give the
            final result as the key and sum of its values*/
            op = output;
            
          while (values.hasNext())
          {
               sum += values.next().get();  
          }
          
          if(sum > maxcount)
          {
            maxword.set(key);
            maxcount = sum;
          }
      }
      
      @Override
      public void close() throws IOException{
    	  op.collect(maxword, new IntWritable(maxcount));
      }
      /*
      @Override
      public void cleanup(Context context)
      throws IOException, InterruptedException {
        System.out.println(maxword);
        context.write(new Text(maxword), new IntWritable(maxcount));
      }*/
      
}
