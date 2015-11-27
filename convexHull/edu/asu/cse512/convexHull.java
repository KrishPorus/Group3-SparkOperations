package edu.asu.cse512;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPoint;

import java.util.*;

public class convexHull
{
    public static void main(String[] args)
    {

			//Handle invalid arguments..
			if(args.length < 2){
				System.out.println("Usage: ConvexHull arg1 arg2");
				System.out.println("arg1: input dataset A file path [points]");
				System.out.println("arg2: output file name and path");
				System.exit(1);
			}

			//Creating and setting sparkconf
			SparkConf sparkConf = new SparkConf().setAppName("Group3-edu.asu.cse512.ConvexHull");
			JavaSparkContext sc = new JavaSparkContext(sparkConf);

			//Adding external jars
			//sc.addJar("lib/jts-1.13.jar");

    	JavaRDD<String> lines = sc.textFile(args[0]);
    	//Using mapPartitions function to find convex hull points in distributed environment
    	JavaRDD<Coordinate> hullPointsRDD = lines.mapPartitions(new ConvexH());
    	List<Coordinate> hullPointsList = hullPointsRDD.collect();
    	Coordinate[] inputArray = new Coordinate[hullPointsList.size()];
    	int j = 0;
    	for(Coordinate c: hullPointsList) {
    		inputArray[j] = c;
    		j++;
    	}
    	//Finding convex hull points on the final subset of points retrieved from distributed environment
    	GeometryFactory geoFactory1 = new GeometryFactory();
    	MultiPoint mPoint1 = geoFactory1.createMultiPoint(inputArray);
    	Geometry geo1 = mPoint1.convexHull();
    	Coordinate[] convexHullResult = geo1.getCoordinates();
    	int length = convexHullResult.length;
    	Coordinate[] convexHullFinalResult = Arrays.copyOf(convexHullResult, length-1);
    	Arrays.sort(convexHullFinalResult);
    	
    	//Converting the list of coordinates into Coordinate RDD
    	JavaRDD<Coordinate> convexHullResultRDD = sc.parallelize(Arrays.asList(convexHullFinalResult), 1);
    	JavaRDD<String> convexHullResultString = convexHullResultRDD.repartition(1).map(new Function<Coordinate, String>(){
			public String call(Coordinate hullPoint) throws Exception {

				return hullPoint.x+","+hullPoint.y;
			}
    		
    	});
    	//Save the String RDD into text file. Using repartition(1) to preserve the order of coordinates
    	convexHullResultString.repartition(1).saveAsTextFile(args[1]);
    }
}

//Function to find convex hull points in distributed environment
@SuppressWarnings("serial")
class ConvexH implements FlatMapFunction<Iterator<String>, Coordinate>
{
	public Iterable<Coordinate> call(Iterator<String> coordinatesIterator) throws Exception {
		// TODO Auto-generated method stub
		List<Coordinate> coorList = new ArrayList<Coordinate>();
		
		//Retrieving points from JavaRDD<String> and storing it in a list
		while(coordinatesIterator.hasNext()){
			String[] temp = coordinatesIterator.next().split(",");
			coorList.add(new Coordinate(Double.parseDouble(temp[0]), Double.parseDouble(temp[1])));
		}
    	Coordinate[] coorArray = new Coordinate[coorList.size()];
     	int i = 0;
    	for(Coordinate c: coorList){
    		coorArray[i] = c;
    		i++;
    	}
    	//Using Geometry class of JTS library to find convex hull points
    	GeometryFactory geoFactory = new GeometryFactory();
    	MultiPoint mPoint = geoFactory.createMultiPoint(coorArray);
    	Geometry geo = mPoint.convexHull();
    	Coordinate[] convexResult = geo.getCoordinates();
    	return Arrays.asList(convexResult);
	}
    
}


