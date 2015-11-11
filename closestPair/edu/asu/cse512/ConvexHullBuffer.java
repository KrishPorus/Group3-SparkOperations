package edu.asu.cse512;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.operation.buffer.BufferOp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by VamshiKrishna on 11/6/2015.
 */
public class ConvexHullBuffer {
    public static ArrayList<ClosestPairPoint> findBoundaryPoints(ArrayList<ClosestPairPoint> originalPoints, double delta, LocalClosestPair localClosestPair) {
        ArrayList<Coordinate> coordinates = new ArrayList<Coordinate>();
        for (ClosestPairPoint p : originalPoints) {
            Coordinate c = new Coordinate(p.getxCoord(),p.getyCoord());
            coordinates.add(c);
        }

        GeometryFactory geometryFactory = new GeometryFactory();
        ConvexHull convexHull = new ConvexHull(coordinates.toArray(new Coordinate[coordinates.size()])
                                                    , geometryFactory);
        Geometry geometry = convexHull.getConvexHull();
        Geometry buffGeom = BufferOp.bufferOp(geometry, -delta);
        List<Coordinate> localConvexHull = Arrays.asList(buffGeom.getCoordinates());
//        for (Coordinate x : localConvexHull) {
//            System.out.println(x.x+","+x.y);
//        }
        coordinates.removeAll(localConvexHull);
        ArrayList<ClosestPairPoint> pointArrayList = new ArrayList<ClosestPairPoint>();
        ClosestPairPoint leftPoint = localClosestPair.getLeftPoint();
        ClosestPairPoint rightPoint = localClosestPair.getRightPoint();
        for (Coordinate x : coordinates) {
            //System.out.println(x.x+","+x.y);
            if (leftPoint.getxCoord() != x.x &&
                    leftPoint.getyCoord() != x.y &&
                    rightPoint.getxCoord() != x.x &&
                    rightPoint.getyCoord() != x.y) {
                ClosestPairPoint p = new ClosestPairPoint(x.x,x.y);
                pointArrayList.add(p);
            }
        }
        //System.out.println(coordinates.size());
        //System.out.println(localConvexHull.size());
        pointArrayList.add(leftPoint);
        pointArrayList.add(rightPoint);
        return pointArrayList;
    }
}
