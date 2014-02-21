package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



public class MultipleOutputMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	public static boolean countFlag=false;
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String page="PageRank.outlink.out";
		String value_in = value.toString();
		String[] input = value_in.split("\\t",4);
		String value_tored=input[0]+"\t"+input[3];
		output.collect(new Text(page), new Text(value_tored));
		if(countFlag==false){
			output.collect(new Text("PageRank.n.out"), new Text(input[2]));
			countFlag=true;
		}
	}
}
