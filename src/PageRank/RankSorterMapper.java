package PageRank;

import java.io.IOException; 

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class RankSorterMapper extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
		String[] input = value.toString().split("\\t",4);
		Double rank = new Double(input[1]);
		Long pageCount = new Long(input[2]);
		Double pageRankThreshold = 5.0/pageCount;
		if(rank > pageRankThreshold)
			output.collect(new DoubleWritable(rank), new Text(input[0]));
	}

}
