// bassi, aman 1001393217
package edu.uta.cse6331;

import java.io.IOException;
import java.util.Scanner;
import java.util.*;
import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

// Vertex class for creating vertex
class Vertex implements Writable{
	int tag;
	long group;
	long vid;
	public Vector<Long> adjacent = new Vector<Long>();
	long list_size;
	Vertex(){}// empty constructor
	// another constructors where setting the values
	Vertex(int tag, long group, long vid, Vector<Long> adjacent){
		this.tag = tag;
		this.group = group;
		this.vid = vid;
		this.adjacent = adjacent;
		list_size = adjacent.size();
	}
	Vertex(int tag,long group){
		this.tag = tag;
		this.group = group;
	}
	// writing to a stream
	public void write(DataOutput dataout) throws IOException{
		dataout.writeInt(tag);
		dataout.writeLong(group);
		dataout.writeLong(vid);
		dataout.writeLong(list_size);
		for(int i=0; i<adjacent.size(); i++){
			dataout.writeLong(adjacent.get(i));
	}
	}
// reading from a stream
	public void readFields(DataInput datainput) throws IOException{
		tag = datainput.readInt();
		adjacent = new Vector<Long>();
		group = datainput.readLong();
		vid = datainput.readLong();
		list_size = datainput.readLong();
		for(long i=0; i<list_size; i++){
			adjacent.add(datainput.readLong());
			}
	}
	
}


// Graph class
public class Graph {
	// first mapper
	public static class Mapper_First extends Mapper<Object, Text, LongWritable, Vertex>{
		@Override// overriding the map from Mapper
//scanning from a file and then mapping it
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			int tag = 0;
			Scanner scan = new Scanner(value.toString()).useDelimiter(",");
			long vid = scan.nextLong();
			Vector<Long> vector = new Vector<Long>();
			while(scan.hasNext()){
				vector.add(scan.nextLong());
			}
			LongWritable long_writ = new LongWritable(vid);
			Vertex ver = new Vertex(tag,vid,vid,vector);
			context.write(long_writ, ver );
			scan.close();
		}
	}
// first reducer
	public static class Reducer_first extends Reducer<LongWritable, Vertex, LongWritable, Vertex>{
		@Override// overriding the reduce
		public void reduce(LongWritable key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException{
					for(Vertex v:values){
						context.write(key,new Vertex(v.tag,v.group, v.vid, v.adjacent) );
					}
		}
		
	}
	
	/*public static class Mapper_second extends Mapper<Object,text, LongWritable, Vertex>{
		@Override
		public void map(Object key, Text value , Context context) throws IOException,InterruptedException{
			StringTokenizer token = new StringTokenizer(value.toString());
			while(token.hasMoreTokens()){
			Long vid = Long.parseLong(token.nextToken());
			
			context.write(new LongWritable(vid), )
				
			}		
			context.write();
		}
	}*/
	// second mapper
	public static class Mapper_second extends Mapper<LongWritable,Vertex, LongWritable, Vertex>{
		@Override
		public void map(LongWritable key, Vertex values, Context context) throws IOException,InterruptedException{
			context.write(new LongWritable(values.vid), values);
			int tag = 1;
			for(long v:values.adjacent){
				context.write(new LongWritable(v), new Vertex(tag, values.group));
			}
		}
	}
	// second reducer
	public static class Reducer_second extends Reducer<LongWritable , Vertex, LongWritable, Vertex>{
		@Override
		public void reduce(LongWritable key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException{
			long m = Long.MAX_VALUE;// finding the maximum value
			
			Vertex vertex = new Vertex();// creating an instance for vertex
			for(Vertex v: values){
				if(v.tag==0){
					 vertex = new Vertex(v.tag, v.group, v.vid, v.adjacent);// if tag is 0 then creating a vertex 
				}
				m = Long.min(m,v.group);// setting m to min value between m and group
			}
			context.write(new LongWritable(m), new Vertex(0, m,vertex.vid, vertex.adjacent));// writing to the output file
		}
	}
	// third mapper
	public static class Mapper_third extends Mapper<LongWritable, Vertex, LongWritable, IntWritable>{
		@Override// overriding the map method of Mapper
		public void map(LongWritable key, Vertex values, Context context) throws IOException, InterruptedException{
			IntWritable val = new IntWritable(1);// creating an instance of IntWritable
			context.write(new LongWritable(values.group), val);// writing to the output file
			}
	}
	// third reducer
	public static class Reducer_third extends Reducer<LongWritable,IntWritable, LongWritable, LongWritable>{
		@Override// overriding the reduce method of reducer
		public void reduce(LongWritable key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
			long m = 0L;// setting the value of long m to 0
			for(IntWritable val:value){
				m = m + Long.valueOf(val.get());
			}
			context.write(key,new LongWritable(m));// setting the key and value of m and writing to the output file
		}
		
	}

    public static void main ( String[] args ) throws Exception {
		// getting the instance for first job
		Job job = Job.getInstance();
	// setting job name
		job.setJobName("graph");
	// set class
		job.setJarByClass(Graph.class);
// setting output key and value class
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Vertex.class);
//setting  mapper output key and value class
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Vertex.class);
		//job.setReducerClass(Reducer_first.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Mapper_First.class);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);// waiting for completion of job 1
		Path in=new Path(args[1]);
 		Path out=null;
 		for(int j=1;j<=5;j++)
 			{	
// getting the instance for second job

 				Job job2=Job.getInstance();
 				job2.setJobName("Map");// setting the name of a job
 				job2.setJarByClass(Graph.class);// setting the class for job
 				out= new Path(args[1]+"/f"+j);
				// setting the map output key and value classes
 				job2.setMapOutputKeyClass(LongWritable.class);
 				job2.setMapOutputValueClass(Vertex.class);
				// set the reducer class
 				job2.setReducerClass(Reducer_second.class);
				// setting the output key and value class
 				job2.setOutputKeyClass(LongWritable.class);
 				job2.setOutputValueClass(Vertex.class);
 				job2.setOutputFormatClass(SequenceFileOutputFormat.class);
 				MultipleInputs.addInputPath(job2,in,SequenceFileInputFormat.class,Mapper_second.class);
 				FileOutputFormat.setOutputPath(job2,out);
 				in=out;
 				job2.waitForCompletion(true);// waiting for the completion of job 2
 			}	
 		
 		Job job3 = Job.getInstance();// getting an instance of job3
		job3.setJobName("job3");// setting the name for job3
		job3.setJarByClass(Graph.class);// setting the class for it
		job3.setReducerClass(Reducer_third.class);// setting the reducer class for it
        job3.setOutputFormatClass(TextOutputFormat.class);
    	// set the output key and value classes
		job3.setOutputKeyClass(LongWritable.class);
      	job3.setOutputValueClass(LongWritable.class);
      	// setting the map output key and value classes
		job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(IntWritable.class);
		
      	MultipleInputs.addInputPath(job3,new Path(args[1]+"/f5"),SequenceFileInputFormat.class,Mapper_third.class);
		FileOutputFormat.setOutputPath(job3,new Path(args[2]));
		job3.waitForCompletion(true);// waiting for the job completion of third job
 	}    
}


		


