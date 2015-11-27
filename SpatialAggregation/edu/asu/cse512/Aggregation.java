package edu.asu.cse512;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Aggregation {

    public static void main(String[] args) {
        //Handling wrong number of parameters
        if(args.length < 3){
            System.out.println("Usage: edu.asu.cse512.Aggregation arg1 arg2 arg3");
            System.out.println("arg1: input dataset A file path[Target points]");
            System.out.println("arg2: input dataset B file path [Query Recntagles]");
            System.out.println("arg3: output file name and path");
            System.exit(1);
        }

        //Creating and setting sparkconf
        SparkConf sparkConf = new SparkConf().setAppName("Group3-edu.asu.cse512.Aggregation");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //Adding external jars
        //sc.addJar("target/SpatialAggregation.jar");
        //sc.addJar("lib/jts-1.13.jar");
        //sc.addJar("lib/guava-18.0.jar");

        //Read from HDFS, the query and the target files
        JavaRDD<String> targetpoints = sc.textFile(args[0]);
        JavaRDD<String> qrectangles = sc.textFile(args[1]);
        final JavaRDD<GeometryWrapper> queries = qrectangles.map(new JoinQueryReadInput());
        List<GeometryWrapper> queryForBroadCast = queries.collect();

        //Broadcast the query rectangles
        final Broadcast<List<GeometryWrapper>> broad_var = sc.broadcast(queryForBroadCast);
        JavaRDD<GeometryWrapper> targetPoints = targetpoints.map(new JoinQueryReadInput());

        //map partitionstopair to find all the pairs of ids of query and targets which form a join.
        JavaPairRDD<Integer, Integer> result = targetPoints.mapPartitionsToPair(new PairFlatMapFunction<Iterator<GeometryWrapper>, Integer, Integer>() {
            public Iterable<Tuple2<Integer, Integer>> call(Iterator<GeometryWrapper> targetIterator) throws Exception {
                List<Tuple2<Integer, Integer>> result = new ArrayList<Tuple2<Integer, Integer>>();
                List<GeometryWrapper> bv = broad_var.getValue();
                while(targetIterator.hasNext()){
                    GeometryWrapper target = targetIterator.next();
                    for(GeometryWrapper query: bv){
                        if(query.getGeometry().contains(target.getGeometry()) ||
                                query.getGeometry().touches(target.getGeometry())){
                            result.add(new Tuple2<Integer, Integer>(query.getId(), 1));
                        }
                    }
                }
                return result;
            }
        });

        JavaPairRDD<Integer, Integer> toPrint = result.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return (integer+integer2);
            }
        });

        JavaRDD<String> filePrint = toPrint.flatMap(new FlatMapFunction<Tuple2<Integer, Integer>, String>() {
            public Iterable<String> call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                List<String> res = new ArrayList<String>();
                String temp = integerIntegerTuple2._1().toString();
                temp += ","+integerIntegerTuple2._2().toString();
                res.add(temp);
                return res;
            }
        }).sortBy(new Function<String, Integer>() {
            public Integer call(String s) throws Exception {
                Integer ret = Integer.parseInt(s.substring(0, s.indexOf(',')).trim());
                return ret;
            }
        }, true, 1);

        //Save output in text file to user provided location from third argument
        filePrint.saveAsTextFile(args[2]);
    }

    public static class JoinQueryReadInput implements Function<String, GeometryWrapper> {
        public GeometryWrapper call(String s) throws Exception {
            String vals[] = s.split(",");
            GeometryWrapper r;
            if(vals.length > 3){
                int id = Integer.parseInt(vals[0]);
                double x1 = Double.parseDouble(vals[1]);
                double y1 = Double.parseDouble(vals[2]);
                double x2 = Double.parseDouble(vals[3]);
                double y2 = Double.parseDouble(vals[4]);
                r = new CustomRectangle(id, x1, y1, x2, y2);
            }else{
                int id = Integer.parseInt(vals[0]);
                double x1 = Double.parseDouble(vals[1]);
                double y1 = Double.parseDouble(vals[2]);
                r = new CustomPoint(id, x1, y1);
            }
            return r;
        }
    }
}

interface GeometryWrapper {
    Geometry getGeometry();
    int getId();
    @Override String toString();
}

class CustomRectangle implements GeometryWrapper, java.io.Serializable{
    private int id;
    private double x1, y1, x2, y2;
    private Polygon rect = null;

    public CustomRectangle(int id, double x1, double y1, double x2, double y2) {
        this.id = id;
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
        this.rect = new Polygon(r, null, new GeometryFactory());
    }

    public Geometry getGeometry() {
        return this.rect;
    }

    public int getId(){
        return id;
    }

    @Override
    public String toString() {
        DecimalFormat df = new DecimalFormat("#.#####");
        return id+":"+df.format(x1)+","+df.format(y1)+","+df.format(x2)+","+df.format(y2);
    }
}

class CustomPoint implements GeometryWrapper, java.io.Serializable{
    private int id;
    private double x1, y1;
    private Point point = null;

    public CustomPoint(int id, double x1, double y1){
        this.id = id;
        this.x1 = x1;
        this.y1 = y1;
        GeometryFactory geometryFactory = new GeometryFactory();
        point = geometryFactory.createPoint(new Coordinate(x1,y1));
    }

    public Geometry getGeometry() {
        return this.point;
    }

    public int getId(){
        return id;
    }

    @Override
    public String toString() {
        DecimalFormat df = new DecimalFormat("#.#####");
        return id+":"+df.format(x1)+","+df.format(y1);
    }

}



