package PageRank;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class RedLinkRemoverReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {
	 
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		Long pageCount = 0L;
		ArrayList<String> outLinks = new ArrayList<String>();
		while (values.hasNext()) {
			String value = values.next().toString();
			String[] input = value.split("\\t");
			outLinks.add(input[0]);
			pageCount= new Long(input[1].toString());
		}
		String outLinkStr = new String();
		double initRank = 1.0/pageCount;
		outLinkStr = String.valueOf(initRank) + "\t" + pageCount.toString();
		Iterator<String> i = outLinks.iterator();
		while(i.hasNext()){
			outLinkStr = outLinkStr + "\t" + i.next();
		}
		output.collect(key, new Text(outLinkStr));
		
	}

}