package edu.asu.cse512;/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.*;

public final class ClosestPair {

  public static void main(String[] args) {

    //Handle invalid arguments..
    if(args.length < 2){
      System.out.println("Usage: edu.asu.cse512.ClosestPair arg1 arg2");
      System.out.println("arg1: input dataset A file path [points]");
      System.out.println("arg2: output file name and path");
      System.exit(1);
    }

    //Creating and setting sparkconf
    SparkConf sparkConf = new SparkConf().setAppName("Group3-edu.asu.cse512.ClosestPair");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    //Adding external jars
    ctx.addJar("lib/jts-1.13.jar");

    //Read from HDFS
    JavaRDD<String> lines = ctx.textFile(args[0]);

    // Create Partitions and calculate local closest pairs.
    JavaRDD<ClosestPairPoint> points = lines.mapPartitions(new FlatMapFunction<Iterator<String>, ClosestPairPoint>() {
      public Iterable<ClosestPairPoint> call(Iterator<String> stringIterator) throws Exception {
        ArrayList<ClosestPairPoint> pointList = new ArrayList<ClosestPairPoint>();
        while (stringIterator.hasNext()) {
          String nextString = stringIterator.next();
          String[] coords = nextString.split(",");
          ClosestPairPoint p = new ClosestPairPoint(Double.parseDouble(coords[0]), Double.parseDouble(coords[1]));
          pointList.add(p);
        }

        LocalClosestPair cP = LocalClosestPair.findClosestPair(pointList);
        ClosestPairPoint leftPoint = cP.getLeftPoint();
        ClosestPairPoint rightPoint = cP.getRightPoint();
        double xDiff = leftPoint.getxCoord() - rightPoint.getxCoord();
        double yDiff = leftPoint.getyCoord() - rightPoint.getyCoord();
        double delta = Math.sqrt((xDiff * xDiff) + (yDiff * yDiff));


        return ConvexHullBuffer.findBoundaryPoints(pointList, delta, cP);
      }
    });

    // Collect all local closest pairs..
    List<ClosestPairPoint> output = points.collect();

    JavaRDD<ClosestPairPoint> finalPoints = ctx.parallelize(output).repartition(1);

    // Calculate final closest pair..
    JavaRDD<ClosestPairPoint> closestPair = finalPoints.mapPartitions(new FlatMapFunction<Iterator<ClosestPairPoint>, ClosestPairPoint>() {
      public Iterable<ClosestPairPoint> call(Iterator<ClosestPairPoint> pointIterator) throws Exception {
        ArrayList<ClosestPairPoint> pointList = new ArrayList<ClosestPairPoint>();
        while (pointIterator.hasNext()) {
          pointList.add(pointIterator.next());
        }
        LocalClosestPair cP = LocalClosestPair.findClosestPair(pointList);
        ClosestPairPoint leftPoint = cP.getLeftPoint();
        ClosestPairPoint rightPoint = cP.getRightPoint();
        ArrayList<ClosestPairPoint> closestPair = new ArrayList<ClosestPairPoint>();
        closestPair.add(leftPoint);
        closestPair.add(rightPoint);
        return closestPair;
      }
    });
    List<ClosestPairPoint> closesPairPoints = closestPair.collect();
    Collections.sort(closesPairPoints, new Comparator<ClosestPairPoint>() {
      public int compare(ClosestPairPoint closestPairPoint, ClosestPairPoint t1) {
        int result = Double.compare(closestPairPoint.getxCoord(), t1.getxCoord());
        if (result == 0) {
          result = Double.compare(closestPairPoint.getyCoord(), t1.getyCoord());
        }
        return result;
      }
    });
    List<String> stringClosestPair = new ArrayList<String>();
    for (ClosestPairPoint p : closesPairPoints) {
      stringClosestPair.add(p.getxCoord() + "," + p.getyCoord());
    }
    JavaRDD<String> finalPair = ctx.parallelize(stringClosestPair);

    //Save final pair into hdfs..
    finalPair.repartition(1).saveAsTextFile(args[1]);
  }
}
