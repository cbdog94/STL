package app;

import algorithm.CombinationDetection;
import bean.Cell;

/**
 * Distance-Time Model
 */
public class OneTrajectory {


    public static void main(String[] args) {

        Cell startPoint = new Cell("[109776,53554]");//陆家嘴
        Cell endPoint = new Cell("[109802,53581]");//中环路交界

        String testID = "0f22bad0cafd467cb4d142661e4d2380";

        CombinationDetection.combination(testID, startPoint, endPoint);

    }


}
