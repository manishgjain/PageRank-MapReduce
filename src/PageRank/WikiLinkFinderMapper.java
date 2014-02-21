package PageRank;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class WikiLinkFinderMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		reporter.incrCounter(PageRank.Counters.INPUT_TITLES, 1);
		String title = findTitle(value);
		//if(title==null)
		//	return;
		//<RedLinkflag> is used to eliminate the red links from the input data
		output.collect(new Text(title), new Text("<RedLinkflag>"));
		ArrayList<String> wikiLinksList = findLinks(value);
		if(wikiLinksList==null){
			return;
		}
		if(!wikiLinksList.isEmpty()){
			for(String s : wikiLinksList){
				output.collect(new Text(s), new Text(title));
			}
		}
		else{
			output.collect(new Text("<NoOutLinkFlag>"), new Text(title));
		}
	}

	//This method finds the Title of the article in the Page Block
	public String findTitle(Text value) throws CharacterCodingException{
		int start = value.find("<title>");
		int end = value.find("</title>", start);
		start+=7;
		String title = Text.decode(value.getBytes(), start, end-start);
		//if(title.contains(":") || title.contains("#") || title.contains("/") )
			//return null;
		//else
			return title.replace(" ", "_");
	}
	
	//This method extracts the Text block from the Page Block, and applies the regex to extract the WikiLinks and performs basic 
	public ArrayList<String> findLinks(Text value) throws CharacterCodingException{
		int start = value.find("<text");
		start = value.find(">", start);
		int end = value.find("</text>", start);
		//start+=1;
		String textBlock=new String();
		try{
		if(end < value.getLength())
			textBlock = Text.decode(value.getBytes(), start, end-start);
		else
			textBlock = Text.decode(value.getBytes(), start, value.getLength()-start);
		} catch(Exception e){
			return null;
		}
		ArrayList<String> List = new ArrayList<String>();
		
		Pattern wikiLinkRegEx = Pattern.compile("\\[\\[(?:[^|\\]]*\\|)?([^\\]]+)\\]\\]");
		Matcher patternMatcher = wikiLinkRegEx.matcher(textBlock);
		while(patternMatcher.find()) {
			int flag=0;
			int startIndex = patternMatcher.start();
			int endIndex = patternMatcher.end();
			String wikiLink = textBlock.substring(startIndex+2, endIndex-2);
			wikiLink = wikiLink.replace(" ", "_");

			//Checking for the occurrence of '|'
			if(wikiLink.contains("|")){
				int pipeIndex = wikiLink.indexOf("|");
				wikiLink = wikiLink.substring(0, pipeIndex);
			}
			
			//Flagging all the invalid WikiLinks 
			//if(wikiLink.contains(":") || wikiLink.contains("#") || wikiLink.contains("/") ){
			//	flag=1;
			//}
			
			// Add the valid WikiLinks in the List
			if(flag==0){
				wikiLink = wikiLink.replace("&amp;", "&");
				List.add(wikiLink);
			}
	    }
		return List;
	}
}
