import java.io.IOException;
import java.util.StringTokenizer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.lang.Number;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.JobContext;

public class Count {

    public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{
	
	private Text word = new Text();

	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
	    // Scanner input_file = new Scanner(value.toString());
	    String line = value.toString();
	    //int year = line.substring(1,5);
			
	    //Store the page, that is linking to the following pages, as a string object - this will be used for the map
	    String year_key = line.substring(1,5);
			
			
		int length = line.length();
		
	    //Store the outlinks in a string array, these will serve as the key's
	    String value_string = line.substring(14,length-1);
		String one = "count";
		
		if(value_string.equals("nan")  ||   value_string.contains("[a-ZA-Z]+")){
			
		}
		else{
			//this is the page rank value
			double num_val = Double.parseDouble(value_string);
		  
		 
			word.set(one);
			context.write(word, new IntWritable(1));
		}
	  

	}
}

	


    public static class PageRankReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
		       ) throws IOException, InterruptedException {
	
	 	 int count = 0;
		 int sum = 0;
		
		for (IntWritable val : values) {
			sum += val.get();
			++count;
		}
		
		result.set(sum);
		context.write(key, result);

	}
	
}

    
    
    

public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    
    //conf.set("filename2", "hdfs:"+args[0]);
    
    Job job = Job.getInstance(conf, "average");
    job.setJobName("average");

    job.setNumReduceTasks(1);

    job.setJarByClass(Count.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(PageRankReducer.class);
    job.setReducerClass(PageRankReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}