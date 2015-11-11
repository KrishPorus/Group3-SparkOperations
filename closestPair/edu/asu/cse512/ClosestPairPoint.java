package edu.asu.cse512;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by VamshiKrishna on 10/15/2015.
 */
public class ClosestPairPoint implements Serializable{
    private double xCoord;
    private double yCoord;

    public ClosestPairPoint(double xCoord, double yCoord) {
        this.xCoord = xCoord;
        this.yCoord = yCoord;
    }

    public double getxCoord(){
        return xCoord;
    }

    public double getyCoord() {
        return yCoord;
    }

    static Comparator<ClosestPairPoint> getXCoordComparator() {
        return new Comparator<ClosestPairPoint>() {
            public int compare(ClosestPairPoint o1, ClosestPairPoint o2)
            {
                if (o1.getxCoord() - o2.getxCoord() >= 0)
                    return 1;
                else
                    return -1;
                //return o1.getxCoord() - o2.getxCoord();
            }
        };
    }

    static Comparator<ClosestPairPoint> getYCoordComparator() {
        return new Comparator<ClosestPairPoint>() {
            public int compare(ClosestPairPoint o1, ClosestPairPoint o2)
            {
                if (o1.getyCoord() - o2.getyCoord() >= 0)
                    return 1;
                else
                return -1;
                //return o1.getyCoord() - o2.getyCoord();
            }
        };
    }
}
