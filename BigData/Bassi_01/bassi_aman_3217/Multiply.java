package edu.uta.cse6331;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

//setting the matrix through 0 or 1, index and values corresponding to it 
class Elem implements Writable{
	short select_matrix;
	int index;
	double value;
	Elem(){}// empty constructor
	Elem(short select_matrix, int index, double value)// constructor for setting the values
	{
		this.select_matrix = select_matrix;
		this.index = index;
		this.value =value;
	}
	// writing to a stream
	public void write(DataOutput dataout) throws IOException{
		dataout.writeInt(index);
		dataout.writeShort(select_matrix);
		dataout.writeDouble(value);
	}
	// reading to the stream
	public void readFields(DataInput data_in) throws IOException{
		index = data_in.readInt();
		select_matrix = data_in.readShort();
		value = data_in.readDouble();
	}
}

// getting rows and column as a pair and using compareTo to check whether the pairs are equal...
class Pair implements WritableComparable<Pair>{
	int index_i;
	int index_j;
	//empty constructor
	Pair(){
	}
	// constructor for setting the rows and columns of a matrix
	Pair(int index_i, int index_j){
		this.index_i = index_i;
		this.index_j = index_j;
	}
	// overriding the compareTO method..
	@Override
	public int compareTo(Pair p)
	{	
		// checking whether the rows are equal
		if (index_i==p.index_i)
		{
			if (index_j == p.index_j)// checking whether the columns are equal
				return 0;
			else if(index_j > p.index_j)
				return 1;
			else
				return -1;
		}
		else if(index_i>p.index_i)
			return 1;
		else 
			return -1;
	}
	// method to return the i j 
	public String toString () { 
		String a = index_i+" "+index_j;
		return a; 
	 }
	// writing to the stream
	public void write(DataOutput dout) throws IOException{
		dout.writeInt(index_i);
		dout.writeInt(index_j);
	}
	// reading from the stream
	public void readFields(DataInput din) throws IOException{
		index_i = din.readInt();
		index_j = din.readInt();
	}
	 
}
// Multiply class where jobs are done
public class Multiply{
	// mapper for the 1st matrix
	public static class Mapper_matrix1 extends Mapper<Object,Text,IntWritable,Elem>{
		@Override
		public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
			// scanning the input and splitting the values as i, j and v based on the delimiter..
			Scanner scan = new Scanner(value.toString()).useDelimiter(",");
			short select_matrix = 0;
			int i = scan.nextInt();
			int j = scan.nextInt();
			double v = scan.nextDouble();
			IntWritable int_writ = new IntWritable(j);
			Elem element = new Elem(select_matrix,i,v);
			context.write( int_writ, element);// writing the result
			scan.close();
			
		}	
	}
	// mapper for the second matrix
	public static class Mapper_matrix2 extends Mapper<Object,Text,IntWritable,Elem>{
		@Override
		public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
			// scanning the input and splitting the values as i, j and v based on the delimiter..
			Scanner scan = new Scanner(value.toString()).useDelimiter(",");
			short select_matrix = 1;
			int i = scan.nextInt();
			int j = scan.nextInt();
			double v = scan.nextDouble();
			IntWritable new_writ = new IntWritable(i);
			Elem element2= new Elem(select_matrix,j,v);
			context.write( new_writ, element2);
			scan.close();
			
		}	
	}
	// 1st reducer for reducing the mappers output
	public static class Reducer_matrix extends Reducer<IntWritable,Elem,Pair,DoubleWritable>{
			static Vector<Elem> maps1 = new Vector<Elem>();// creating the vector for the ist matrix
			static Vector<Elem> maps2 = new Vector<Elem>();// creating the vector for the 2nd matrix
			@Override
			public void reduce(IntWritable index, Iterable<Elem> values, Context context) throws IOException, InterruptedException{
				maps1.clear();
				maps2.clear();
				// iterating over the values of ELem type
				for(Elem v:values){
					if(v.select_matrix==0)// since its==0 that means 1st matrix
					{	
						Elem e =new Elem(v.select_matrix,v.index,v.value);
						maps1.add(e);//adding the object to the vector
					}
					else
					{	
						v.select_matrix = 1;// since its==1 that means 2nd matrix
						Elem e =new Elem(v.select_matrix,v.index, v.value);
						maps2.add(e);//adding the object to the vector
					}
				} 
				for(Elem a:maps1){
					for(Elem b:maps2){
						Pair pair =new Pair(a.index,b.index);
						DoubleWritable doublewrit = new DoubleWritable(a.value*b.value);
						context.write(pair, doublewrit);// writing the pair and the product of their elem object values
						
					}
				}
			}
		
	
	
	}
	// mapping the output of job1 and taking input as text
	public static class Mapper_red extends Mapper<Object, Text,Pair, DoubleWritable>{
			@Override
			public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
				StringTokenizer token = new StringTokenizer(value.toString());
	        	while (token.hasMoreTokens()) {
	        		int i = Integer.parseInt(token.nextToken());
	    			int j = Integer.parseInt(token.nextToken());
	    			double v = Double.parseDouble(token.nextToken());
					Pair pair =new Pair(i,j);
					DoubleWritable doublewritable = new DoubleWritable(v);
	            	context.write(pair, doublewritable);
					
	        	}	
			}
	}
	// reducer to reduce the output of mapper to text and doublewritable
	public static class Reducer_map extends Reducer<Pair, DoubleWritable,Text, DoubleWritable> {
			@Override
			public void reduce(Pair pair, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
				double sum = 0;
				for(DoubleWritable v: values){
					sum += v.get();
					
				}
				String final_key = Integer.toString(pair.index_i) + " " + Integer.toString(pair.index_j);
				Text text =new Text(final_key);
				DoubleWritable newdoublewritable = new DoubleWritable(sum);
				context.write(text,newdoublewritable);
			}
		
	}
	// main method
	public static void main(String[] args) throws Exception{
		// getting the instance fro job1 and setting its name
		Job job = Job.getInstance();
        job.setJobName("Multiply");
        // creating the jar and setting the output key and value class
		job.setJarByClass(Multiply.class);
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);
        // setting the mappers output key and value class
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Elem.class);
        // setting the reducer class and adding the input path of matrix 1 and matrix2 to job1 through arguments
        job.setReducerClass(Reducer_matrix.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Mapper_matrix1.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,Mapper_matrix2.class);
		// setting the output path for job1
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
			// checking for the success of job1
		boolean success = job.waitForCompletion(true);
			//getting the instance and setting the job name
			if(success){
			Job job2 = Job.getInstance();
			job2.setJobName("Summing up the  Values");
			job2.setJarByClass(Multiply.class);// setting the jar
			// setting output key and value class
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(DoubleWritable.class);
			// setting the mapper class and output key and value class for the mapper
			job2.setMapperClass(Mapper_red.class);
			job2.setMapOutputKeyClass(Pair.class);
	        job2.setMapOutputValueClass(DoubleWritable.class);
			// setting the reducer class
	        job2.setReducerClass(Reducer_map.class);
			// setting the output format
	        job2.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job2, new Path(args[2]));
			FileOutputFormat.setOutputPath(job2,new Path(args[3]));
			job2.waitForCompletion(true);
	}
}
	
}