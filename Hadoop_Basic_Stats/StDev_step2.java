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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StDev_step2 {

	
    public static class TokenizerMapper
	extends Mapper<Object, Text, Text, DoubleWritable>{
	
	private Text word = new Text();  //word
	private Text value = new Text();  //value

	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
				
	    // Scanner input_file = new Scanner(value.toString());
	    String line = value.toString();
	    //int year = line.substring(1,5);	
			
		int length = line.length();
		
	    //Store the outlinks in a string array, these will serve as the key's
	    String value_string = line.substring(9,length-1);
		String one = "standard deviation";
		
		if(value_string.equals("nan")){
			
		}
		else{
			//this is the page rank value
			double num_val = Double.parseDouble(value_string);
			word.set(one);
			context.write(word, new DoubleWritable(num_val));
		}
	  

	}
}

	


    public static class PageRankReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

	private Text new_key = new Text();
	private DoubleWritable result = new DoubleWritable();
	
	public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
		       ) throws IOException, InterruptedException {
				
	 	 double count = 0.0;
		double sum = 0.0;
		
		for (DoubleWritable val : values) {
			//String string_val = val.toString();
			sum = sum + (val.get()*val.get()); //Double.parseDouble(string_val);
			++count;
		}
		double temp = sum;
		result.set(Math.sqrt(sum/count));
		//key.set(Integer.toString(COUNT));
		context.write(key, result);
	}
	
}


public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "stdev_2");
    job.setJobName("stdev_2");
	
	
    job.setNumReduceTasks(1);

    job.setJarByClass(StDev_step2.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(PageRankReducer.class);
    job.setReducerClass(PageRankReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
	
	
	FileInputFormat.setInputPaths(job, new Path(args[0])); //, new Path(conf.get("GDP_file")), new Path(conf2.get("average_file")));  //, new Path(conf2.get("average_file")));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}