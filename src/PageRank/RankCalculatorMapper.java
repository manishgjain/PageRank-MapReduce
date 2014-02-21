package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class RankCalculatorMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		int pageIndex = values.find("\t");
		int rankIndex = values.find("\t",pageIndex+1);
		int pageCountIndex = values.find("\t",rankIndex+1);

		String page = Text.decode(values.getBytes(), 0, pageIndex);
        String pageWithRank = Text.decode(values.getBytes(), 0, rankIndex+1);
        String pageCountWithRank = Text.decode(values.getBytes(),0, pageCountIndex+1);
        pageWithRank = pageWithRank.trim();
        String[] count = pageCountWithRank.split("\\t");
 
        output.collect(new Text(page), new Text("<exists>"+"\t"+count[2]));
 
        // Skip pages with no links.
        if(rankIndex == -1) return;
        
        int valueLength = values.getLength();
        String outLinksText = Text.decode(values.getBytes(), pageCountIndex+1, valueLength -(pageCountIndex+1));
        String[] outLinksArray = outLinksText.split("\t");
        int outLinksCount = outLinksArray.length;
 
        for (String outLink : outLinksArray){
            Text rankAndLinkCount = new Text(pageWithRank + "\t" +outLinksCount + "\t");
            output.collect(new Text(outLink), rankAndLinkCount);
        }
 
        // Put the original links of the page for the reduce output
        output.collect(new Text(page), new Text("|"+outLinksText));
	}

}
