
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.CoordinateSequenceFactory;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequenceFactory;
import com.vividsolutions.jts.algorithm.ConvexHull;

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
			SparkConf sparkConf = new SparkConf().setAppName("Group3-ConvexHull");
			JavaSparkContext sc = new JavaSparkContext(sparkConf);

			//Adding external jars
			sc.addJar("lib/jts-1.13.jar");

    	JavaRDD<String> lines = sc.textFile(args[0]);
    	JavaRDD<Coordinate> hullPointsRDD = lines.mapPartitions(new ConvexH());
    	List<Coordinate> hullPointsList = hullPointsRDD.collect();
    	Coordinate[] inputArray = new Coordinate[hullPointsList.size()];
    	int j = 0;
    	for(Coordinate c: hullPointsList) {
    		inputArray[j] = c;
    		j++;
    	}
    	GeometryFactory geoFactory1 = new GeometryFactory();
    	MultiPoint mPoint1 = geoFactory1.createMultiPoint(inputArray);
    	Geometry geo1 = mPoint1.convexHull();
    	Coordinate[] convexHullResult = geo1.getCoordinates();
    	
    	JavaRDD<Coordinate> convexHullResultRDD = sc.parallelize(Arrays.asList(convexHullResult), 1);
    	JavaRDD<String> convexHullResultString = convexHullResultRDD.repartition(1).map(new Function<Coordinate, String>(){
			public String call(Coordinate hullPoint) throws Exception {
				// TODO Auto-generated method stub
				return hullPoint.x+","+hullPoint.y;
			}
    		
    	});
    	convexHullResultString.repartition(1).saveAsTextFile(args[1]);
    }
}


@SuppressWarnings("serial")
class ConvexH implements FlatMapFunction<Iterator<String>, Coordinate>
{
	public Iterable<Coordinate> call(Iterator<String> coordinatesIterator) throws Exception {
		// TODO Auto-generated method stub
		List<Coordinate> coorList = new ArrayList<Coordinate>();
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
    	
    	GeometryFactory geoFactory = new GeometryFactory();
    	MultiPoint mPoint = geoFactory.createMultiPoint(coorArray);
    	Geometry geo = mPoint.convexHull();
    	Coordinate[] convexResult = geo.getCoordinates();
    	return Arrays.asList(convexResult);
	}
    
}


