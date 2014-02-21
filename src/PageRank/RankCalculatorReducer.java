package PageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class RankCalculatorReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

	private static final float dampFactor = 0.85F;
	private Long pageCount = 0L;
    
    public void reduce(Text page, Iterator<Text> values, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
        boolean wikiPageExists = false;
        String[] split;
        float SumOfpageRankOfInLink = 0;
        String links = "";
        String pageWithRank;
    	String[] count;
        
        while(values.hasNext()){
            pageWithRank = values.next().toString();
        
            // Marking only those Links which were found in the <title> tag
            if(pageWithRank.startsWith("<exists>")) {
            	wikiPageExists = true;
            	count = pageWithRank.split("\\t");
            	pageCount = new Long(count[1]);
                continue;
            }
            
            // Storing all the outlinks of the WikiLink for further iterations.
            if(pageWithRank.startsWith("|")){
                links = "\t"+pageWithRank.substring(1);
                continue;
            }
            
            split = pageWithRank.split("\\t");
            
            float pageRankOfLink = Float.valueOf(split[1]);
            int countOutLinks = Integer.valueOf(split[2]);
            SumOfpageRankOfInLink += (pageRankOfLink/countOutLinks);
        }

        if(!wikiPageExists) 
        	return;
        //Evaluating the page rank of the current WikiLink(key) using the outLinksCount and inLinkRank
        float newRank = dampFactor * SumOfpageRankOfInLink + (1-dampFactor)/pageCount;

        out.collect(page, new Text(newRank + "\t" + pageCount.toString() + links));
    }
}

