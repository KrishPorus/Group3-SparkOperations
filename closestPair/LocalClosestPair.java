import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by VamshiKrishna on 10/15/2015.
 */
public class LocalClosestPair {
    private final ClosestPairPoint pair1;
    private final ClosestPairPoint pair2;

    public LocalClosestPair(ClosestPairPoint pair1, ClosestPairPoint pair2) {
        this.pair1 = pair1;
        this.pair2 = pair2;
    }

    public ClosestPairPoint getLeftPoint() {
        return pair1;
    }

    public ClosestPairPoint getRightPoint() {
        return pair2;
    }

    public static LocalClosestPair findClosestPair(ArrayList<ClosestPairPoint> pointList) {
        //Implement the algorithm here..
        Collections.sort(pointList, ClosestPairPoint.getXCoordComparator());
        ArrayList<ClosestPairPoint> ySortList = new ArrayList<ClosestPairPoint>(pointList);
        Collections.sort(ySortList, ClosestPairPoint.getYCoordComparator());
        return (findClosestInSorted(pointList, ySortList));
    }

    private static LocalClosestPair findClosestInSorted(List<ClosestPairPoint> xSortedList, List<ClosestPairPoint> ySortedList) {
        int listSize = xSortedList.size();
        if (listSize <= 3) {
            return bruteForce(xSortedList);
        }
        int mid = xSortedList.size()/2;
        ClosestPairPoint midPoint = xSortedList.get(mid);
        ArrayList<ClosestPairPoint> yListLeft = new ArrayList<ClosestPairPoint>();
        ArrayList<ClosestPairPoint> yListRight = new ArrayList<ClosestPairPoint>();
        int li = 0;
        int ri = 0;
        for (int i = 0; i < ySortedList.size(); ++i) {
            if (ySortedList.get(i).getxCoord() <= midPoint.getxCoord()) {
                yListLeft.add(li++,ySortedList.get(i));
            } else {
                yListRight.add(ri++,ySortedList.get(i));
            }
        }

        LocalClosestPair leftPair = findClosestInSorted(xSortedList.subList(0, mid), yListLeft);
        LocalClosestPair rightPair = findClosestInSorted(xSortedList.subList(mid,xSortedList.size()), yListRight);

        double leftPairDistance = distance(leftPair.getLeftPoint(), leftPair.getRightPoint());
        double rightPairDistance = distance(rightPair.getLeftPoint(), rightPair.getRightPoint());

        double delta = Math.min(leftPairDistance, rightPairDistance);
        ArrayList<ClosestPairPoint> closePoints = new ArrayList<ClosestPairPoint>();
        int j = 0;
       for (int i = 0; i < ySortedList.size(); i++) {
           if (Math.abs(ySortedList.get(i).getxCoord() - midPoint.getxCoord()) < delta) {
               closePoints.add(j,ySortedList.get(i));
               j++;
           }
       }

        LocalClosestPair stripClose = stripClosest(closePoints, delta);
        if (stripClose != null) {
            double stripDist = distance(stripClose.getLeftPoint(),stripClose.getRightPoint());
            if (stripDist < delta)
                return stripClose;
        }

        if (leftPairDistance < rightPairDistance)
            return leftPair;
        else
            return rightPair;
    }

    private static LocalClosestPair stripClosest(ArrayList<ClosestPairPoint> strip, double delta) {
        double min = delta;
        ClosestPairPoint p1 = null;
        ClosestPairPoint p2 = null;
        for (int i = 0; i < strip.size(); i++) {
            for (int j = i+1; j < strip.size() && (strip.get(j).getyCoord() - strip.get(i).getyCoord()) < min ; ++j) {
                double dist = distance(strip.get(i), strip.get(j));
                if (dist < min) {
                    min = dist;
                    p1 = strip.get(i);
                    p2 = strip.get(j);
                }
            }
        }
        if (p1 == null || p2 == null)
            return null;
        return new LocalClosestPair(p1,p2);
    }

    private static double distance(ClosestPairPoint p1, ClosestPairPoint p2) {
        return Math.sqrt(((p1.getxCoord() - p2.getxCoord())*(p1.getxCoord() - p2.getxCoord())) +
                ((p1.getyCoord() - p2.getyCoord()) * (p1.getyCoord() - p2.getyCoord())));
    }

    private static LocalClosestPair bruteForce(List<ClosestPairPoint> pointList) {
        double min = Integer.MAX_VALUE;
        ClosestPairPoint p1 = null;
        ClosestPairPoint p2 = null;
        for (int i = 0; i < pointList.size(); ++i) {
            for (int j = i+1; j < pointList.size(); ++j) {
                double dist = distance(pointList.get(i), pointList.get(j));
                if (dist < min) {
                    min = dist;
                    p1 = pointList.get(i);
                    p2 = pointList.get(j);
                }
            }
        }
        return (new LocalClosestPair(p1, p2));
    }
}
