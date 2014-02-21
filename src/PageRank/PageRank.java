package PageRank;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
//import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class PageRank {
	static enum Counters { INPUT_TITLES };
	
	
	static class MultiFileOutput extends MultipleTextOutputFormat<Text, Text> {
		public static boolean isLong(String s) {
		    try { 
		        Long.parseLong(s); 
		    } catch(NumberFormatException e) { 
		        return false; 
		    }
		    // only got here if we didn't return false
		    return true;
		}
        protected String generateFileNameForKeyValue(Text key, Text value,String name) {
        	if(isLong(key.toString())){
              	return "PageRank.n.out";
            }
            else 
            	return "PageRank.outlink.out";
        }
	}

	public static void main(String[] args) throws Exception {
		PageRank PR = new PageRank();
		//For input : replace </user/manishgjain/> with the public spring IDS bucket.
		//For output : replace </user/manishgjain/> with the our bucket name.
		//Extract WikiLinks and eliminate RedLinks. Also, prepare data for the Power Iterations
		//s3n://spring-2014-ds
		PR.runWikiLinkFinder("s3n://spring-2014-ds"+"/data/",args[0]+"/tmp/phase1", args[0]+"/tmp/iter0");
		//Generate the output files PageRank.outlink.out(adjacency list) and PageRank.n.out(total page count)
		PR.generateOutputFiles(args[0]+"/tmp/iter0", args[0]+"/tmp/results");
		//Perform the power iteration method 8 times to generate the PageRank of each page.
		int iterations=8;
		for(int currentIteration=0;currentIteration<iterations;currentIteration++)
			PR.runPowerIteration(args[0]+"/tmp/iter"+currentIteration, args[0]+"/tmp/iter"+(currentIteration+1),currentIteration+1,args[0]+"/results");
		//Generate the output file PageRank.iter0.out(human readable file consisting of Pageranks with page titles for iteration 1) 
		PR.sortPageRank(args[0]+"/tmp/iter1",args[0]+"/tmp/sorter/1", args[0]+"/results/PageRank.iter1.out",false,args[0]);
		//Generate the output file PageRank.iter8.out(human readable file consisting of Pageranks with page titles for iteration 8)
		PR.sortPageRank(args[0]+"/tmp/iter8",args[0]+"/tmp/sorter/8", args[0]+"/results/PageRank.iter8.out",true,args[0]);
	}
	
	//Method to run the MapReduce job for finding the WikiLinks and removing RedLinks from the input .xml File
	public void runWikiLinkFinder(String inputPath, String intermediatePath, String outputPath) throws IOException{

		//Running the phase-1a i.e. Parse XML and find RedLinks from the set of WikiLinks 
		JobConf conf = new JobConf(PageRank.class);
		conf.setJarByClass(PageRank.class);
		
		//Set Mapper properties
		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		
		conf.setInputFormat(XmlInputFormat.class);
		conf.set(XmlInputFormat.START_TAG_KEY,"<page>");
		conf.set(XmlInputFormat.END_TAG_KEY,"</page>");
		conf.setMapperClass(WikiLinkFinderMapper.class);
		
		
		// Set Reducer properties
		FileOutputFormat.setOutputPath(conf, new Path(intermediatePath));
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
		conf.setReducerClass(WikiLinkFinderReducer.class);
		
		//set the job client
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//Running the phase-1b i.e. Format the Graph for the powerIteration method, without RedLinks
		JobConf conf1 = new JobConf(PageRank.class);
		conf1.setJarByClass(PageRank.class);
		
		//Set Mapper properties
		FileInputFormat.setInputPaths(conf1, new Path(intermediatePath));
		conf1.setInputFormat(TextInputFormat.class);
		conf1.setMapperClass(RedLinkRemoverMapper.class);
		
		// Set Reducer properties
		FileOutputFormat.setOutputPath(conf1, new Path(outputPath));
		conf1.setOutputFormat(TextOutputFormat.class);
		//conf1.setOutputFormat(MultiFileOutput.class);
		conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(Text.class);
		conf1.setReducerClass(RedLinkRemoverReducer.class);
		//conf1.setNumReduceTasks(2);
		//set the job client
		try {
			JobClient.runJob(conf1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	
	
	//Method to run the MapReduce job which generates the files PageRank.outlink.out and PageRank.n.out 
	public void generateOutputFiles(String inputPath,String outputPath) throws IOException{
		//Running the MapReduce job for generating the PageRank.outlink.out and PageRank.n.out files.
		JobConf conf2 = new JobConf(PageRank.class);
		conf2.setJarByClass(PageRank.class);
		
		//Set Mapper properties
		FileInputFormat.setInputPaths(conf2, new Path(inputPath));
		conf2.setInputFormat(TextInputFormat.class);
		conf2.setMapperClass(MultipleOutputMapper.class);
		
		// Set Reducer properties
		FileOutputFormat.setOutputPath(conf2, new Path(outputPath));
		conf2.setOutputFormat(MultiFileOutput.class);
		conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(Text.class);
		conf2.setReducerClass(MultipleOutputReducer.class);
		conf2.setNumReduceTasks(2);
		//set the job client
		try {
			JobClient.runJob(conf2);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public void runPowerIteration(String inputPath, String outputPath, int iteration, String resultPath) {
        JobConf conf = new JobConf(PageRank.class);
        
		conf.setJarByClass(PageRank.class);

		//Set Mapper properties
		FileInputFormat.setInputPaths(conf, new Path(inputPath));
        conf.setInputFormat(TextInputFormat.class);
		conf.setMapperClass(RankCalculatorMapper.class);
		
		
		// Set Reducer properties
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setReducerClass(RankCalculatorReducer.class);
//      conf.setNumReduceTasks(0);
		
  		//set the job client
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
//		if(iteration == 1 || iteration == 8){
//			//Code for merging into one file
//			try{
//			FileSystem fs = FileSystem.get(conf);
//			FileUtil.copyMerge(fs, new Path(outputPath), fs, new Path(resultPath+"/PageRank.iter"+iteration+".out"), false,new Configuration( conf),"");
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
	}
	
	
	public void sortPageRank(String inputPath,String outputPath, String resultPath, boolean copy, String bucket_name) throws IOException{
		//Running the MapReduce job for generating the PageRank.outlink.out and PageRank.n.out files.
		JobConf conf = new JobConf(PageRank.class);
		conf.setJarByClass(PageRank.class);
		
		//Set Mapper properties
		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		conf.setInputFormat(TextInputFormat.class);
		conf.setMapOutputKeyClass(DoubleWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setMapperClass(RankSorterMapper.class);
		
		// Set Reducer properties
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
		conf.setReducerClass(RankSorterReducer.class);
		conf.setNumReduceTasks(1);
		conf.setOutputKeyComparatorClass(SortKeyComparator.class);
		//set the job client
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try{
			FileSystem fs = FileSystem.get(new URI(bucket_name+"/results/"), conf); 
			FileUtil.copyMerge(fs, new Path(outputPath), fs, new Path(resultPath), false,new Configuration( conf),"");
			if(copy){
				FileUtil.copy(fs, new Path(bucket_name+"/tmp/results/PageRank.outlink.out"), fs, new Path(bucket_name+"/results/PageRank.outlink.out"), true,true,new Configuration( conf));
				FileUtil.copy(fs, new Path(bucket_name+"/tmp/results/PageRank.n.out"), fs, new Path(bucket_name+"/results/PageRank.n.out"),true,true,new Configuration( conf));
				System.out.print("inside if");
//				fs.rename(new Path(bucket_name+"/tmp/results/PageRank.outlink.out"), new Path(resultPath+"/results/PageRank.outlink.out"));
//				fs.rename(new Path(bucket_name+"/tmp/results/PageRank.n.out"), new Path(resultPath+"/results/PageRank.n.out"));
			}
			} catch (Exception e) {
				e.printStackTrace();
			}
	}


}
