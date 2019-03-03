import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class EQLog extends Configured implements Tool  {

	public static class EQMapper extends Mapper<Object, Text, Text, DoubleWritable> 
	{
		public  Text outkey=new Text();
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
	        String[] line = value.toString().split(",");
	       if(line.length!=12)
	       {
	    	   System.out.println("Log entry is invalid");
	    	   return;
	       }
	       else
	       {
	    	String outputkey=new String();
	    	outputkey=line[11];
	    	double outputValue =  Double.parseDouble(line[8]);
	        context.write(new Text(outputkey), new DoubleWritable(outputValue));
	       }
	    }
	}
	
public static class EQReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		
	
	
	
	 @Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context ) throws IOException, InterruptedException {
		  
	    	double max_magnitude= Double.MIN_VALUE;
	    	 for(DoubleWritable val:values)
	    	 {
	        max_magnitude=Math.max(val.get(), max_magnitude);
	    	 }
	    	
	        context.write(key,new DoubleWritable(max_magnitude));
	        }
}
		   
@Override
public int run(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	            
	    if (args.length != 2) {
	        System.err.println("Input and Output Directory not specified.");
	        System.exit(0);      
	      }
	    
	   
	    Job job = new Job(conf);
        job.setJarByClass(EQLog.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.setMapperClass(EQMapper.class);
	    job.setReducerClass(EQReduce.class);
	    
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class)	;
	
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
		job.setNumReduceTasks(1);    
	  
	  
		

		return (job.waitForCompletion(true) ? 0 : 1);	
}
	
		
public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new Configuration(), new EQLog(), args));
	}


	    }
	

