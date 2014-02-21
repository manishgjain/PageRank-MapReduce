package PageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MultipleOutputReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {
	 
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String page = key.toString();
		
		if(page.equals("PageRank.outlink.out")){
			while (values.hasNext()) {
				String value = values.next().toString();
				output.collect( new Text(value),new Text(""));
			}
		}
		else{
			output.collect( new Text(values.next().toString()),new Text(""));
		}
		
		
	}

}
