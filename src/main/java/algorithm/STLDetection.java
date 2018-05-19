package algorithm;

import bean.GPS;
import org.apache.commons.math3.analysis.polynomials.PolynomialFunction;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import util.CommonUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * STL algorithm
 *
 * @author Bin Cheng
 */
public class STLDetection {

    private static final int DEGREE = 3;
    private static final double THRESHOLD = 0.9;
    private static Map<Integer, double[]> cacheWT = new ConcurrentHashMap<>();
    private static Map<Integer, double[]> cacheWD = new ConcurrentHashMap<>();
    private static Map<Integer, Double> cacheET = new ConcurrentHashMap<>();
    private static Map<Integer, Double> cacheED = new ConcurrentHashMap<>();

    public static double detect(List<List<GPS>> allNormalTrajectories, List<GPS> testTrajectory) {
        return detect(allNormalTrajectories, testTrajectory, THRESHOLD, THRESHOLD, DEGREE);
    }

    public static double detect(List<List<GPS>> allNormalTrajectories, List<GPS> testTrajectory, double thresholdTime, double thresholdDist, int degree) {

        List<Double> linearDistance = new ArrayList<>();//x
        List<Double> intervals = new ArrayList<>();//y
        List<Double> totalDistance = new ArrayList<>();//y2

        double[] wt, wd;
        double et, ed;
        double linearDist, drivingDist, segment, drivingTime;

        Date startTime = testTrajectory.get(0).getTimestamp();
        Calendar calendar = GregorianCalendar.getInstance(); // creates a new calendar instance
        calendar.setTime(startTime);   // assigns calendar to given date
        int testStartHour = calendar.get(Calendar.HOUR_OF_DAY);

        if (!cacheWT.containsKey(testStartHour)) {

            //get LDT of each point
            for (List<GPS> oneTrajectory : allNormalTrajectories) {
                GPS startPoint = oneTrajectory.get(0);
                GPS lastPoint = oneTrajectory.get(0);
                drivingDist = 0;

                //get start hour
                calendar.setTime(startPoint.getTimestamp());
                int currentStartHour = calendar.get(Calendar.HOUR_OF_DAY);

                //filter
                if (currentStartHour < testStartHour - 1 ||
                        currentStartHour > testStartHour + 1)
                    continue;

                for (GPS currentPoint : oneTrajectory) {
                    linearDist = CommonUtil.distanceBetween(startPoint, currentPoint);
                    segment = CommonUtil.distanceBetween(lastPoint, currentPoint);
                    drivingTime = (currentPoint.getTimestamp().getTime() - startPoint.getTimestamp().getTime()) / 1000;
                    drivingDist += segment;
                    linearDistance.add(linearDist);
                    intervals.add(drivingTime);
                    totalDistance.add(drivingDist);

                    lastPoint = currentPoint;
                }
            }

            if (linearDistance.size() == 0) {
                System.out.println("no train set!");
                return 1;
            }

            //Fitted the data set by maximum likelihood estimation
            double[][] x = new double[linearDistance.size()][degree + 1];
            for (int i = 0; i < linearDistance.size(); i++) {
                for (int j = 0; j < degree + 1; j++) {
                    x[i][j] = Math.pow(linearDistance.get(i), j);
                }
            }

            double[] t = new double[intervals.size()];
            double[] d = new double[totalDistance.size()];
            for (int i = 0; i < intervals.size(); i++) {
                t[i] = intervals.get(i);
                d[i] = totalDistance.get(i);
            }

            RealMatrix X = MatrixUtils.createRealMatrix(x);
            RealMatrix T = MatrixUtils.createColumnRealMatrix(t);
            RealMatrix D = MatrixUtils.createColumnRealMatrix(d);
            RealMatrix XT = X.transpose();
            RealMatrix TT = T.transpose();
            RealMatrix DT = D.transpose();
            wt = MatrixUtils.inverse(XT.multiply(X)).multiply(XT).multiply(T).getColumn(0);
            wd = MatrixUtils.inverse(XT.multiply(X)).multiply(XT).multiply(D).getColumn(0);
            et = (TT.multiply(T).subtract(TT.multiply(X).multiply(MatrixUtils.createColumnRealMatrix(wt)))).getEntry(0, 0) / x.length;
            ed = (DT.multiply(D).subtract(DT.multiply(X).multiply(MatrixUtils.createColumnRealMatrix(wd)))).getEntry(0, 0) / x.length;
            cacheWT.put(testStartHour, wt);
            cacheWD.put(testStartHour, wd);
            cacheET.put(testStartHour, et);
            cacheED.put(testStartHour, ed);
        } else {
            wt = cacheWT.get(testStartHour);
            wd = cacheWD.get(testStartHour);
            et = cacheET.get(testStartHour);
            ed = cacheED.get(testStartHour);
        }

        PolynomialFunction polyT = new PolynomialFunction(wt);
        PolynomialFunction polyD = new PolynomialFunction(wd);

        //detect the incoming trajectory
        List<GPS> anomalyPoints = new ArrayList<>();
        double anomalyScore = 0;
        GPS startPos = testTrajectory.get(0);
        GPS lastPos = testTrajectory.get(0);
        drivingDist = 0;
        for (GPS gps : testTrajectory) {
            linearDist = CommonUtil.distanceBetween(startPos, gps);
            segment = CommonUtil.distanceBetween(lastPos, gps);
            drivingTime = (gps.getTimestamp().getTime() - startTime.getTime()) / 1000;
            drivingDist += segment;
            double predictT = polyT.value(linearDist);
            double predictD = polyD.value(linearDist);

            double cdfT = new NormalDistribution(predictT, Math.sqrt(et)).cumulativeProbability(drivingTime);
            double cdfD = new NormalDistribution(predictD, Math.sqrt(ed)).cumulativeProbability(drivingDist);

            if (cdfT > thresholdTime && cdfD > thresholdDist)
                anomalyPoints.add(gps);

            anomalyScore += segment / (1 + Math.exp(200 * Math.max(thresholdTime - cdfT, thresholdDist - cdfD)));

            lastPos = gps;
        }

        return anomalyPoints.size() * 1.0 / testTrajectory.size();
    }

}
