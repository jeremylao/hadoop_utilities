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

public class StDev {

	
    public static class TokenizerMapper
	extends Mapper<Object, Text, Text, Text>{
	
	private Text the_key = new Text();  //word
	private Text value = new Text();  //value

	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
				
	    String line;
	  
	    line = value.toString();
	 
	    String year_key = line.substring(1,5);
		String check_key[] = line.split("\\s"); //(0,7);
		int length = line.length();
		
	    String value_string = line.substring(14,length-1);
		
		if(value_string.equals("nan") || year_key.equals("date") || check_key[0].equals("count") || check_key[0].equals("average")){
			
		}
		else{
			year_key = year_key+line.substring(6,8)+line.substring(9,11); //+","+average_from_file[1];
			//this is the page rank value
			double num_val = Double.parseDouble(value_string);
		  
			value.set(value_string);
			the_key.set(year_key);
			context.write(the_key, value);   //new DoubleWritable(num_val));
		}
	  

	}
}

	


    public static class PageRankReducer extends Reducer<Text,Text,Text,Text> {

	private Text new_key = new Text();
	private Text result = new Text();
	
	public void reduce(Text key, Iterable<Text> values,
                       Context context
		       ) throws IOException, InterruptedException {
		
		
		 
	    Path pt = new Path("hdfs:/user/jjl359/FRED/GDP/average/part-r-00000");
	    FileSystem fs = FileSystem.get(context.getConfiguration());
	    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	    String line;
	    line=br.readLine();
		
		String average_from_file[] = line.split("\\s");
				
	 	double count = 0.0;
		double sum = 0.0;
		String new_key_string="";
		
		
		for (Text val : values) {
			String string_val = val.toString();
			sum += Double.parseDouble(string_val); //val.get();
			Double temp = Double.parseDouble(average_from_file[1])/2;
			sum = (sum - temp);
			++count;
		}
		String output_val = Double.toString(sum/count);
		result.set(output_val);
		context.write(key, result);

	}
	
}


public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "stdev_1");
    job.setJobName("stdev_1");
	
	
    job.setNumReduceTasks(1);

    job.setJarByClass(StDev.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(PageRankReducer.class);
    job.setReducerClass(PageRankReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
	
	
	FileInputFormat.setInputPaths(job, new Path(args[0])); //, new Path(conf.get("GDP_file")), new Path(conf2.get("average_file")));  //, new Path(conf2.get("average_file")));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}