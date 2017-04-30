import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
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

public class LATITUDE_LONGITUDE_BOUNDING {

    public static class TokenizerMapper
	extends Mapper<Object, Text, Text, Text>{
		
	private Text word = new Text();
	private Text value = new Text();

	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
	   
		//Create a file that 
	    String line = value.toString();
			
	    //Store the page, that is linking to the following pages, as a string object - this will be used for the map
	    String line_components[] = line.split("\t");
		
		String uuid = line_components[15];
		String year = line_components[16];
		
		if(line_components[4].matches(".*[a-zA-Z]+.*") || line_components[5].matches(".*[a-zA-Z]+.*") || line_components[4].trim().isEmpty() || line_components[5].trim().isEmpty()){
		}
		else{
			
			
			if( (24.7433195 <= Double.parseDouble(line_components[4])) && (Double.parseDouble(line_components[4])<= 43.48) &&  (-124.7844079 <=  Double.parseDouble(line_components[5])) && (Double.parseDouble(line_components[5]) <=  -66.9513812)){
				value.set("USA"+"\t"+year);
				word.set(uuid);
				context.write(word, value);
			}
			
			else if( (49.97948 <= Double.parseDouble(line_components[4])) && (Double.parseDouble(line_components[4])<= 58.906) &&  (-8.525390 <=  Double.parseDouble(line_components[5])) && (Double.parseDouble(line_components[5]) <=  1.8013812)){
				value.set("UK"+"\t"+year);
				word.set(uuid);
				context.write(word, value);
			}
			
			else if( (54.82633195 <= Double.parseDouble(line_components[4])) && (Double.parseDouble(line_components[4])<= 68.926811) &&  (4.83398 <=  Double.parseDouble(line_components[5])) && (Double.parseDouble(line_components[5]) <=  21.005859)){
				value.set("SCANDI"+"\t"+year);
				word.set(uuid);
				context.write(word, value);
			}
			
			else if( (43.48 <= Double.parseDouble(line_components[4])) && (Double.parseDouble(line_components[4])<= 63.9) &&  (-129.7844079 <=  Double.parseDouble(line_components[5])) && (Double.parseDouble(line_components[5]) <=  -51.192) ){
				value.set("CANADA"+"\t"+year);
				word.set(uuid);
				context.write(word, value);
			}
			
			else if( (-48.690 <= Double.parseDouble(line_components[4])) && (Double.parseDouble(line_components[4])<= -10.878) &&  (113.2367844079 <=  Double.parseDouble(line_components[5])) && (Double.parseDouble(line_components[5]) <=  178.9513812)){
				value.set("ANZAC"+"\t"+year);
				word.set(uuid);
				context.write(word, value);
			}
			
			else if( (21.7433195 <= Double.parseDouble(line_components[4])) && (Double.parseDouble(line_components[4])<= 42.3457688) &&  (87.7844079 <=  Double.parseDouble(line_components[5])) && (Double.parseDouble(line_components[5]) <=  144.9513812)){
				value.set("FarEast"+"\t"+year);
				word.set(uuid);
				context.write(word, value);
			}
			else if( (7.7433195 <= Double.parseDouble(line_components[4])) && (Double.parseDouble(line_components[4])<= 34.3457688) &&  (67.7844079 <=  Double.parseDouble(line_components[5])) && (Double.parseDouble(line_components[5]) <=  88.34)){
				value.set("INDIA"+"\t"+year);
				word.set(uuid);
				context.write(word, value);
			}
			else if( (-35.7433195 <= Double.parseDouble(line_components[4])) && (Double.parseDouble(line_components[4])<= 35.3457688) &&  (-17.7844079 <=  Double.parseDouble(line_components[5])) && (Double.parseDouble(line_components[5]) <=  61.34)){
				value.set("MiddleEast_Africa"+"\t"+year);
				word.set(uuid);
				context.write(word, value);
			}
			else if( (36.35948 <= Double.parseDouble(line_components[4])) && (Double.parseDouble(line_components[4])<= 49.926) &&  (-9.575390 <=  Double.parseDouble(line_components[5])) && (Double.parseDouble(line_components[5]) <=  16.4313812)){
				value.set("WestEurope-France"+"\t"+year);
				word.set(uuid);
				context.write(word, value);
			}
			else if( (49.926 <= Double.parseDouble(line_components[4])) && (Double.parseDouble(line_components[4])<= 54.35) &&  (2.2890 <=  Double.parseDouble(line_components[5])) && (Double.parseDouble(line_components[5]) <=  23.4313812)){
				value.set("BenNePol-Germany"+"\t"+year);
				word.set(uuid);
				context.write(word, value);
			}
			else if( (-55.3926 <= Double.parseDouble(line_components[4])) && (Double.parseDouble(line_components[4])<= 25.09) &&  (-111.37 <=  Double.parseDouble(line_components[5])) && (Double.parseDouble(line_components[5]) <=  -31.4313812)){
				value.set("C&S Americas"+"\t"+year);
				word.set(uuid);
				context.write(word, value);
			}
			
			else{
				value.set("ROW"+"\t"+year);
				word.set(uuid);
				context.write(word, value);
			}
		}
		
		

	}
}

    public static class ReduceReducer
	extends Reducer<Text,Text,Text,Text> {
		
	private Text result = new Text();

	public void reduce(Text key, Iterable<Text> values,
                       Context context
		       ) throws IOException, InterruptedException {
		
		
		String temp = "";
		for (Text val : values) {
			temp = val.toString();
			//String value_of_key[] = temp.split(",");
			if(temp.equals(",")  || temp.equals("\\s+")){
				
			}
			else{
				//String latitude = value_of_key[0];
				//String longitude = value_of_key[1];
				
				//if( (24.7433195 <= Double.parseDouble(latitude)) && (Double.parseDouble(latitude)<= 49.3457688) &&  (-124.7844079 <=  Double.parseDouble(longitude)) && (Double.parseDouble(longitude) <=  -66.9513812)){
					result.set(temp);
					context.write(key,result);
				//}
				//else{
				//	result.set("ROW");
				//	context.write(key,result);
				//}
			}
		}
	
		
	}
	
}

    
    
    

public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    
    //conf.set("filename2", "hdfs:"+args[0]);
    
    Job job = Job.getInstance(conf, "LATITUDE_LONGITUDE_BOUNDING");
    job.setJobName("LATITUDE_LONGITUDE_BOUNDING");

    job.setNumReduceTasks(1);

    job.setJarByClass(LATITUDE_LONGITUDE_BOUNDING.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(ReduceReducer.class);
    job.setReducerClass(ReduceReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}