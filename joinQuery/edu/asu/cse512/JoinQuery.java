package edu.asu.cse512;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JoinQuery {

    public static void main(String[] args) {
        //Handling wrong number of parameters
        if(args.length < 3){
            System.out.println("Usage: edu.asu.cse512.JoinQuery arg1 arg2 arg3");
            System.out.println("arg1: input dataset A file path[Target Recntagles or points]");
            System.out.println("arg2: input dataset B file path [Query Recntagles]");
            System.out.println("arg3: output file name and path");
            System.exit(1);
        }

        //Creating and setting sparkconf
        SparkConf sparkConf = new SparkConf().setAppName("Group3-edu.asu.cse512.JoinQuery");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //Adding external jars
        //sc.addJar("target/dds-1.0-SNAPSHOT.jar");
        //sc.addJar("lib/jts-1.13.jar");
        //sc.addJar("lib/guava-18.0.jar");


        //Read from HDFS, the query and the target files
        JavaRDD<String> targetFile = sc.textFile(args[0]);
        JavaRDD<String> queryFile = sc.textFile(args[1]);
        JavaRDD<GeometryWrapper> queries = queryFile.map(new JoinQueryReadInput());
        List<GeometryWrapper> queryForBroadCast = queries.collect();

        //Broadcast the query rectangles
        final Broadcast<List<GeometryWrapper>> broad_var = sc.broadcast(queryForBroadCast);
        JavaRDD<GeometryWrapper> targets = targetFile.map(new JoinQueryReadInput());

        //Based on the objects created, decide if target file consists of rectangles or
        //points and broadcast the choice.
        int choice;
        if(targets.first().getGeometry() instanceof Point)
            choice = 1;
        else
            choice = 2;
        final Broadcast<Integer> brChoice = sc.broadcast(choice);

        //map partitionstopair to find all the pairs of ids of query and targets which form a join.
        JavaPairRDD<Integer, Integer> result = targets.mapPartitionsToPair(new PairFlatMapFunction<Iterator<GeometryWrapper>, Integer, Integer>() {
            public Iterable<Tuple2<Integer, Integer>> call(Iterator<GeometryWrapper> targetIterator) throws Exception {
                List<Tuple2<Integer, Integer>> result = new ArrayList<Tuple2<Integer, Integer>>();
                List<GeometryWrapper> bv = broad_var.getValue();
                Integer choice = brChoice.getValue();
                while(targetIterator.hasNext()){
                    GeometryWrapper target = targetIterator.next();
                    for(GeometryWrapper query: bv){
                        if(choice == 2 && (query.getGeometry().intersects(target.getGeometry())
                                || query.getGeometry().contains(target.getGeometry())
                                || target.getGeometry().contains(query.getGeometry())
                                || query.getGeometry().equals(target.getGeometry()))){
                            result.add(new Tuple2<Integer, Integer>(query.getId(), target.getId()));
                        }else{
                            if(choice == 1 && (query.getGeometry().contains(target.getGeometry())
                                || query.getGeometry().intersects(target.getGeometry()))){
                                result.add(new Tuple2<Integer, Integer>(query.getId(), target.getId()));
                            }
                        }
                    }
                }
                return result;
            }
        });

        //combine the pairs by key, to form list of query to target mappings
        JavaPairRDD<Integer, Iterable<Integer>> toPrint = result.groupByKey();
        JavaRDD<String> filePrint = toPrint.flatMap(new FlatMapFunction<Tuple2<Integer, Iterable<Integer>>, String>() {
            public Iterable<String> call(Tuple2<Integer, Iterable<Integer>> integerIterableTuple2) throws Exception {
                List<String> res = new ArrayList<String>();
                String temp = integerIterableTuple2._1().toString();
                int count = 0;
                for (Integer id : integerIterableTuple2._2()) {
                    count++;
                    temp += "," + id;
                }
                if(count == 0)
                    temp = ",NULL";
                res.add(temp);
                return res;
            }
        }).sortBy(new Function<String, Integer>() {
            public Integer call(String s) throws Exception {
                return Integer.parseInt(s.substring(0, s.indexOf(",")));
            }
        }, true, 1);

        //User below code for debugging to print the output to local console
        //List<String> resFinal = filePrint.collect();
        //for(String res: resFinal){
        //    System.out.println(res);
        //}

        //Save output in text file to user provided location from third argument
        System.out.println("Saving the output in:"+args[2]);
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



