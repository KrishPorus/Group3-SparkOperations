package edu.asu.cse512;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.lang.String;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

class PointPair implements Serializable{
    private Coordinate p1;
    private Coordinate p2;
    private double pointDistance;
    public PointPair(Coordinate p1, Coordinate p2) {
        this.p1 = p1;
        this.p2 = p2;
    }
    public double distance() {
        return p1.distance(p2);
    }
    
    public double getPointDistance() {
        return pointDistance;
    }
    public String getCoordinates() {
    	return p1.x +","+ p1.y +"\n" +p2.x+","+p2.y;
    }
    public void setPointDistance(double pointDistance) {
        this.pointDistance = pointDistance;
    }
}

public class FarthestPair
{
    public static void main(String[] args) {

        //Handle invalid arguments..
        if(args.length < 2){
            System.out.println("Usage: edu.asu.cse512.FarthestPair arg1 arg2");
            System.out.println("arg1: input dataset A file path [points]");
            System.out.println("arg2: output file name and path");
            System.exit(1);
        }

        //Creating and setting sparkconf
        SparkConf sparkConf = new SparkConf().setAppName("Group3-edu.asu.cse512.FarthestPair");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //Adding external jars
        //sc.addJar("lib/jts-1.13.jar");

        JavaRDD<String> textFile = sc.textFile(args[0]);
        JavaRDD<Coordinate> coordinates = textFile.flatMap(
                new FlatMapFunction<String, Coordinate>() {
                    public Iterable<Coordinate> call(String s) {
                        String[] coordinate = s.split(",");
                        double xCoord = Double.parseDouble(coordinate[0]);
                        double yCoord = Double.parseDouble(coordinate[1]);
                        return Arrays.asList(new Coordinate(xCoord, yCoord));
                    }
                }
        );
        // get Convex Hull Points
        JavaRDD<Coordinate> hullPoints = coordinates.mapPartitions(new myConvexHull());
        JavaRDD<Coordinate> reducedPoints = hullPoints.repartition(1);
        JavaRDD<Coordinate> finalPoints = reducedPoints.mapPartitions(new myConvexHull());
        List<Coordinate> convexHullPoints = finalPoints.collect();
        Coordinate[] chPoints = convexHullPoints.toArray(new Coordinate[convexHullPoints.size()]);
        Coordinate p1,p2;
        List<PointPair> allPairs = new ArrayList<PointPair>();

        // Form all pairs of Points
        for (int i = 0; i < chPoints.length-1; i++) {
            for (int j = i+1; j< chPoints.length; j++) {
                PointPair obj = new PointPair(chPoints[i], chPoints[j]);
                allPairs.add(obj);
            }
        }
        // Distribute poitns to workers and calculate the distance
        JavaRDD<PointPair> pointPairs = sc.parallelize(allPairs);
        JavaRDD<PointPair> farthestPair = pointPairs.mapPartitions(new CalculateDistance());
        List<PointPair> farthestPointPairs = farthestPair.collect();
        Iterator<PointPair> itr = farthestPointPairs.iterator();
        double maxDistance = 0;
        PointPair result = null;
        while (itr.hasNext()) {
            PointPair obj = itr.next();
            if (obj == null) {
                System.out.println("Hello world");
            }
            if (obj.getPointDistance() > maxDistance) {
                maxDistance = obj.getPointDistance();
                result = obj;
            }
        }
        if (result == null) {
            System.out.println("Result object is null");
        }
        ArrayList<String> listOfPoints = new ArrayList();
        listOfPoints.add(result.getCoordinates());
        JavaRDD<String> finalresult = sc.parallelize(listOfPoints);

        finalresult.repartition(1).saveAsTextFile(args[1]);
    }

    static class myConvexHull implements FlatMapFunction<Iterator<Coordinate>, Coordinate>, Serializable
    {
        public Iterable<Coordinate> call(Iterator<Coordinate> pointIterator) throws Exception {
            List<Coordinate> currentPoints = new ArrayList<Coordinate>();
            try {
                while (pointIterator.hasNext()) {
                    currentPoints.add(pointIterator.next());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            // Create a convexHull class with constructor
            GeometryFactory geomFactory = new GeometryFactory();
            ConvexHull ch = new ConvexHull(currentPoints.toArray(new Coordinate[currentPoints.size()]), geomFactory);
            // get coordinates of the convex hull using getConvexHull
            Geometry geometry = ch.getConvexHull();
            Coordinate[] c = geometry.getCoordinates();

            //Convert the coordinates array to arraylist here
            List<Coordinate> pts = Arrays.asList(c);
            return pts;
        }
    }

    private static class CalculateDistance implements FlatMapFunction<Iterator<PointPair>, PointPair> {
        public Iterable<PointPair> call(Iterator<PointPair> pointPairIterator) throws Exception {
            ArrayList<PointPair> farthestPair = new ArrayList<PointPair>();
            try {
                double maxDistance = 0;
                while (pointPairIterator.hasNext()) {
                    PointPair obj = pointPairIterator.next();
                    obj.setPointDistance(obj.distance());
                    if (maxDistance < obj.getPointDistance()) {
                        maxDistance = obj.getPointDistance();
                        farthestPair.clear();
                        farthestPair.add(obj);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("Size :: " + farthestPair.size());
            return farthestPair;
        }
    }
}
