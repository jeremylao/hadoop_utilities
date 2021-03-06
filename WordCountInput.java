import java.io.IOException;
import java.util.StringTokenizer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
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

public class PassingInputParameters {

    public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable zero = new IntWritable(0);
	private Text word = new Text();

	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
	    Scanner input = new Scanner(value.toString());

	    while(input.hasNextLine()){
	            
		String line = input.nextLine();

		if(line.substring(0,1).matches("[0-9]+")){
		    String date = line.substring(0,9);
		    String date_delim = "[-]";
		    String[] date2 = date.split(date_delim);
		    word.set(date2[1]);
		    context.write(word,one);
	            
		    int start_name_index = line.indexOf("#");
		    int end_name_index = line.indexOf(",",line.indexOf(",")+1);
		    String name = line.substring(start_name_index+1,end_name_index);
		    word.set(name);
		    context.write(word,one);
	            
		    String message = line.substring(end_name_index+1, line.length());
		    String message2 = message.replaceAll("[^a-zA-Z0-9^'\\s]"," ").replaceAll("\\s+", " ");
		    String message_words[] = message2.split(" ");
	            
      
		    for(int i = 0 ; i<message_words.length; ++i){
			word.set(message_words[i]);
			context.write(word,one);
		    }
	      

		}

		else if(line.substring(0,1).matches("[A-Za-z]")){
		      
		    String message_input = line.replaceAll("[^a-zA-Z0-9']"," ").replaceAll("\\s+", " ");
		    String message_words_input[] = message_input.split(" ");
		    
		    for(int i = 0 ; i<message_words_input.length; ++i){
			message_words_input[i].replaceAll("\\s+", "");
		    }
		        
		    for(int i = 0 ; i<message_words_input.length; ++i){
			word.set(message_words_input[i]);
			context.write(word,zero);
		    }
		    
		} 
	    }     
	}
    }


    public static class IntSumReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
		       ) throws IOException, InterruptedException {
	
	    Configuration conf = context.getConfiguration();
	       
	    String path_name = conf.get("filename2");
	    
	    Path pt = new Path(path_name);
	    FileSystem fs = FileSystem.get(new Configuration());
	    
	    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	    String line;
	    line=br.readLine();
        

	    String message_input = line.replaceAll("[^a-zA-Z0-9']"," ").replaceAll("\\s+", " ");
	    String message_words_input[] = message_input.split(" ");
		    
	    for(int i = 0 ; i<message_words_input.length; ++i){
		message_words_input[i].replaceAll("\\s+", "");
	    }
	    
	    int sum = 0;
	    for (IntWritable val : values) {
		sum += val.get();
	    }
	    result.set(sum);
	
	    if(key.toString().equals(message_words_input[0]) || key.toString().equals(message_words_input[1]) || key.toString().equals(message_words_input[2]) || key.toString().equals(message_words_input[3])){
		context.write(key, result);
	    }
	}
    }
    
    
    

public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Configuration conf2 = new Configuration();

    conf2.set("file1", "hdfs:"+args[0]);
    conf.set("filename2", "hdfs:"+args[2]);
    
    Job job = Job.getInstance(conf, "word count");
    job.setJobName("word count");

    job.setNumReduceTasks(1);

    job.setJarByClass(WordCountInput.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.setInputPaths(job, new Path(conf.get("filename2")), new Path(conf2.get("file1")));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
