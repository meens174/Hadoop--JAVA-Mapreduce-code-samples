import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class UserLog extends Configured implements Tool 
{
	public static long maxtime=0;
	public static long mintime=0;
	
	
	public static class ULMapper extends Mapper<Object, Text, Text, LongWritable> 
	{
		public  Text outkey=new Text();
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
	        String[] line = value.toString().split("\\s+");
	        Text outputKey = new Text(line[0]);
	        // outkey.set(outputKey);
	        long outputValue =  Long.valueOf(line[1]).longValue();
	        context.write(outputKey, new LongWritable(outputValue));
	    }
	}
	public static class ULReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		
		public static long startTime;
	    public static long endTime;
	/* @Override
	    public void configure(Job job) {
			 mintime = Long.parseLong(job.get("startTime"));
			 maxtime = Long.parseLong(job.get("endTime"));
		    }*/
	 @Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context ) throws IOException, InterruptedException {
		 Configuration conf = context.getConfiguration();
		  mintime =  Long.parseLong(conf.get("startTime"));
		  maxtime = Long.parseLong(conf.get("endTime"));
		   
	    	int counters=0;
	    	 while (values.iterator().hasNext()) {
	    	        long thisValue = values.iterator().next().get();
	    	      	if((thisValue>=mintime) && (thisValue<=maxtime))
	    		 {
	        counters+=1;
	      
	        }
				
	    	}
	        context.write(key,new LongWritable(counters));
	        }
		
		   
	    }
	 
	
@Override
public int run(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	            
	    if (args.length != 4) {
	        System.err.println("startTime and endTime parameters must be specified");
	        System.exit(0);      
	      }
	    
	   
	    String startTime=args[2];
	    String endtime=args[3];	 
	    	 
	    conf.set("startTime", startTime);
	    conf.set("endTime", endtime);
	    Job job = new Job(conf);
        job.setJarByClass(UserLog.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.setMapperClass(ULMapper.class);
	    job.setReducerClass(ULReduce.class);
	    
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class)	;
	
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    
		job.setNumReduceTasks(1);    
	   // job.setInputFormatClass(text.class);
	    //job.setOutputFormatClass(Text.class);  
		
	  
		

		return (job.waitForCompletion(true) ? 0 : 1);	
}
	
		
public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new Configuration(), new UserLog(), args));
	}
}
   
	

