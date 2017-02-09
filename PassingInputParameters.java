import java.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.JobContext;

public class PassingInputParameters {
	
	public static class Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	
	    Configuration conf = context.getConfiguration();
	    String path_name = conf.get("filename2");
	    Path pt = new Path(path_name);
	    FileSystem fs = FileSystem.get(new Configuration());
	    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	    String line;
	    line=br.readLine();
        

		}
	}
}
    

public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    conf.set("filename2", "hdfs:"+args[2]);
    
}

