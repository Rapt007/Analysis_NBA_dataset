// Aman Bassi(1001393217)
// Ms- CS

package edu.uta.cse6331

import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD._
//import org.apache.spark.rdd.RDD
import java.lang.Math._


object Source {
  def main(args: Array[ String ]) {
// configuring the spark and setting the app name 
	val conf = new SparkConf().setAppName("Shortest");
	val sc = new SparkContext(conf);
// reading the textFile and map all the lines and then splitting the lines into 3 values ie i, d, j
	var matrix_dist = sc.textFile(args(0)).map(x=>{val a= x.split(",")
													(a(0).toInt, a(1).toInt,a(2).toInt)})
	
	val max_val:Int = 9999999 // setting the maximum value to be 9999999
						
// grouping the values bases on j or a(2) values where j would be the key and values would be the group of i,d,j values where j would be same
	val new_r = matrix_dist.groupBy(_._3)
	//new_r.foreach(println)
	
// mapping every values previously read from the file to i, d, j ,9999999
	val adjacent = matrix_dist.map(x => (x._1,x._2, x._3, max_val))
	
	//adjacent.foreach(println)
	//calculating the initial distance that is 0 for i = 0 else it is set to the max_val
	var result = new_r.map(x => if (x._1 == 0) (x._1,0) else (x._1, max_val))
	//result.foreach(println)
	
	// running the loop four times
	for (i <- 1 to 4){
	// mapping the adjacent to (j, and whole value) and similarly the result to the first column value and the whole value and then joining these 2 mappings and mapping it to basically i,d,j,max values which is 0 for 0 else 9999999
	val adjacency = adjacent.map(x =>(x._3,x)).join(result.map(y=>(y._1,y)))
		.map { case (j,(x,y)) => (x._1, x._2, x._3, y._2) } 
	//adjacent.foreach(println)
	//println("adjacent")
	// mapping the adjacent to its first column ie i values and whole values and result to first column and whole values and then joining them based on their keys and then mapping it to j as key and minimum value for this node from the node 0
	result = adjacency.map(x =>(x._1, x)).join(result.map(y => (y._1, y)))
		.map{ case (j, (x,y)) => (x._3, min( y._2 + x._2, x._4))}
	// reducing the result throught the key
	result = result.reduceByKey(_ min _)
	
	}
	// first sorting the result throught the first column of the result and then filtering those values for whom the min values is still the maximum values
	result = result.sortBy(_._1).filter(x => (x._2 != max_val))
	// finally printing the values here
	result.collect.foreach(println)
    sc.stop()
	
}
}