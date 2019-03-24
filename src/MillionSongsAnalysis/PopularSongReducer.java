package MillionSongsAnalysis;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
 
public class PopularSongReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	//private int max = 0;
	//private Text popular = new Text("");
	HashMap<Text, IntWritable> playcountmap = new HashMap<Text, IntWritable>();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException {
		int c = 0;
		for (IntWritable val : values) {
            c += val.get();
        }
	    /*	    
	    if(c>=max)
	    {
	    	max = c;
	    	popular.set(key);
	    }	*/
		playcountmap.put(new Text(key), new IntWritable(c));
	}
	
	@Override
	  public void cleanup(Context context)
	      throws IOException, InterruptedException {
		
		Map<Text, IntWritable> sortedMap = sortByValues(playcountmap);

        int counter = 0;
        for (Text key : sortedMap.keySet()) {
            if (counter++ == 10) {
                break;
            }
            context.write(key, sortedMap.get(key));
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
