package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class RedLinkRemoverMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
		String value_in = values.toString();
		String[] input = value_in.split("\\t");
		Text out_key = new Text(input[0]);
		if(input[1].equals("<NoOutLinkFlag>")){
			output.collect(out_key, new Text(" "+"\t"+input[2]));
		}
		else{
			output.collect(out_key, new Text(input[1]+"\t"+input[2]));
		}
	}

}
