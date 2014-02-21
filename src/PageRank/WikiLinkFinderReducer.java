package PageRank;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;

public class WikiLinkFinderReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

	private Long InputPageCounter;
	
	@Override
	public void configure(JobConf conf){
		try {
			JobClient client;
			client = new JobClient(conf);
			RunningJob parentJob = client.getJob(JobID.forName(conf.get("mapred.job.id")));
			InputPageCounter = parentJob.getCounters().getCounter(PageRank.Counters.INPUT_TITLES);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException  {
	
		String page = key.toString();
		String pageWithCount = page+"\t"+InputPageCounter.toString();
		boolean notRedLink = false;
		HashSet<String> inLinkSet = new HashSet<String>();
		if(page.equals("<NoOutLinkFlag>")){
			notRedLink=true;
			while (values.hasNext()) {
				String value = values.next().toString();
				inLinkSet.add(value);
			}
		}
		else{
			while (values.hasNext()) {
				String value = values.next().toString();
				if(value.contains("<RedLinkflag>")){
					notRedLink=true;
				}
				else{
					inLinkSet.add(value);
				}
			}
		}
		if(!notRedLink){
			return;
		}
		Iterator<String> i =inLinkSet.iterator();
		while(i.hasNext()){
				output.collect(new Text(i.next()), new Text(pageWithCount));
		}
	}

}
