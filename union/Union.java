

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import org.apache.hadoop.fs.*;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.*;
import java.util.*;

class Rectangle{
    private double x1, y1, x2, y2;
    private Polygon p = null;
    public Rectangle(double x1, double y1, double x2, double y2) {
        this.x1 = x1;
        this.y1 = y1;
        this.x2 = x2;
        this.y2 = y2;

        Coordinate rect[] = new Coordinate[5];

        rect[0] = new Coordinate(x1, y1);
        rect[1] = new Coordinate(x2, y1);
        rect[2] = new Coordinate(x2, y2);
        rect[3] = new Coordinate(x1, y2);
        rect[4] = new Coordinate(x1, y1);

        LinearRing r = new LinearRing(new CoordinateArraySequence(rect), new GeometryFactory());
        p = new Polygon(r, null, new GeometryFactory());
    }

    public Polygon getPolygon(){
        return this.p;
    }
}


public class Union {
    public static void main(String[] args){

        //Handle invalid arguments..
        if(args.length < 2){
            System.out.println("Usage: Union arg1 arg2");
            System.out.println("arg1: input dataset A file path [points]");
            System.out.println("arg2: output file name and path");
            System.exit(1);
        }

        //Creating and setting sparkconf
        SparkConf sparkConf = new SparkConf().setAppName("Group3-Union");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //Adding external jars
        sc.addJar("lib/jts-1.13.jar");

        JavaRDD<String> data = sc.textFile(args[0]);

        JavaRDD<Polygon> rects = data.map(new ReadInput());

        rects.foreach(new MyPrinter());

        JavaRDD<Geometry> union = rects.mapPartitions(new ComputeUnion());

        List<Geometry> unions = union.collect();

        Geometry un = null;
        int i = 0;
        for(Geometry x : unions){
            if(i == 0){
                un = x;
                i++;
            }
            un = un.union(x);
            //
        }
        if(un!=null)
            System.out.println("Union soln: " + un.toString());

        JavaRDD<Coordinate> coords =    sc.parallelize(Arrays.asList(un.getCoordinates()),1);
        JavaRDD<String> output = coords.map(new Function<Coordinate, String>() {
            public String call(Coordinate coordinate) throws Exception {
                return coordinate.x + "," + coordinate.y;
            }
        });

        output.repartition(1).saveAsTextFile(args[1]);

    }


    private static class ReadInput implements Function<String, Polygon> {
        public Polygon call(String s) throws Exception {

            String vals[] = s.split(",");

            double x1 = Double.parseDouble(vals[0]);
            double y1 = Double.parseDouble(vals[1]);
            double x2 = Double.parseDouble(vals[2]);
            double y2 = Double.parseDouble(vals[3]);

            Rectangle r = new Rectangle(x1, y1, x2, y2);

            return r.getPolygon();
        }
    }

    private static class ComputeUnion implements org.apache.spark.api.java.function.FlatMapFunction<Iterator<Polygon>, Geometry> {
        public Iterable<Geometry> call(Iterator<Polygon> polygonIterator) throws Exception {
            Set<Geometry> mySet = new HashSet<Geometry>();
            Geometry geo = null;
            int i = 0;
            while(polygonIterator.hasNext()){
                if(i == 0){
                    geo = polygonIterator.next();
                    i++;
                } else {
                    geo = geo.union(polygonIterator.next());
                }
            }
            mySet.add(geo);
            //Iterator<Geometry> itr = mySet.iterator();
            return mySet;
        }
    }

    private static class MyPrinter implements VoidFunction<Polygon> {
        public void call(Polygon polygon) throws Exception {
            System.out.println(polygon.toString());
        }
    }
}