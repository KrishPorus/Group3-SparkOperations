package edu.asu.cse512;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;

class Rangequeryrectangle
{
double x;
double y;
double x1;
double y1;
Rangequeryrectangle(double x,double y,double x1,double y1)
 {
	this.x = x;
	this.y = y;
	this.x1 = x1;
	this.y1 = y1;
 }
public String toString()
{
		return x+","+y+","+x1+","+y1;
}
}

class Rangequerypoint
{
double x;
double y;
Rangequerypoint(double x,double y)
 {
	this.x = x;
	this.y = y;
 }
public String toString()
{
		return x+","+y;
}
}

public class RangeQuery {
	public static boolean compare(Rangequerypoint p1, Rangequeryrectangle r1)
	{
		if((r1.x <= p1.x) && (p1.x <= r1.x1) && (r1.y <= p1.y ) && (p1.y <= r1.y1))
			return true;
		else
		return false;
	}
	public static void main(String[] args){

		//Handle invalid arguments..
		if(args.length < 3){
			System.out.println("Usage: Range Query arg1 arg2 arg3");
			System.out.println("arg1: input dataset A file path [range query input dataset]");
			System.out.println("arg2: input dataset A file path [query window]");
			System.out.println("arg3: output file name and path");
			System.exit(1);
		}

		//Creating and setting sparkconf
		SparkConf sparkConf = new SparkConf().setAppName("Group3-edu.asu.cse512.RangeQuery");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> rqfile = sc.textFile(args[0]);
    	JavaRDD<String> qwfile = sc.textFile(args[1]);
    	List<String> str = qwfile.collect();
    	final Broadcast<List<String>> broad_var = sc.broadcast(str);
    	JavaRDD<String> out = rqfile.map(new Function<String, String>() {
			public String call(String s) {
				List<String> compare = broad_var.value();
				String qw = compare.get(0);
				String[] query_rect = qw.split(",");
				String[] test_data = s.split(",");
				double x = Math.min(Double.parseDouble(query_rect[0]), Double.parseDouble(query_rect[2]));
				double y = Math.min(Double.parseDouble(query_rect[1]), Double.parseDouble(query_rect[3]));
				double x1 = Math.max(Double.parseDouble(query_rect[0]), Double.parseDouble(query_rect[2]));
				double y1 = Math.max(Double.parseDouble(query_rect[1]), Double.parseDouble(query_rect[3]));
				Rangequeryrectangle query = new Rangequeryrectangle(x, y, x1, y1);

				double x2 = Double.parseDouble(test_data[1]);
				double y2 = Double.parseDouble(test_data[2]);
				Rangequerypoint test = new Rangequerypoint(x2, y2);
				if (compare(test, query))
					return test_data[0];
				else
					return "null";
			}
		});
    	JavaRDD<String> filter = out.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				if (s != "null")
					return true;
				else
					return false;
			}
		});

		filter.repartition(1).saveAsTextFile(args[2]);
	}
}
