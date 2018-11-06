package algorithm;

import bean.GPS;
import com.google.common.collect.Maps;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import util.CommonUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * STL algorithm
 *
 * @author Bin Cheng
 */
public class STLOpDetection {

    //    private static final int DEGREE = 3;
//    private static final double THRESHOLD = 0.9;
    private static Map<Integer, Map<Integer, NormalDistribution>> cacheDisplacementModel = new ConcurrentHashMap<>();
    private static Map<Integer, Map<Integer, NormalDistribution>> cacheDistanceModel = new ConcurrentHashMap<>();
    private static Map<Integer, Map<Integer, NormalDistribution>> cacheTimeModel = new ConcurrentHashMap<>();
//    private static Map<Integer, double[]> cacheWD = new ConcurrentHashMap<>();
//    private static Map<Integer, Double> cacheET = new ConcurrentHashMap<>();
//    private static Map<Integer, Double> cacheED = new ConcurrentHashMap<>();
//
//    public static double detect(List<List<GPS>> allNormalTrajectories, List<GPS> testTrajectory) {
//        return detect(allNormalTrajectories, testTrajectory, THRESHOLD, THRESHOLD, DEGREE);
//    }

    private List<List<GPS>> filterByTime(List<List<GPS>> allNormalTrajectories, int startHour) {
        Calendar calendar = GregorianCalendar.getInstance();
        return allNormalTrajectories.stream().filter(x -> {
            calendar.setTime(x.get(0).getTimestamp());
            int currentStartHour = calendar.get(Calendar.HOUR_OF_DAY);
            return currentStartHour < startHour - 1 ||
                    currentStartHour > startHour + 1;
        }).collect(Collectors.toList());
    }

    private void process(List<GPS> trajectory, Map<Integer, List<Double>> distanceMap, Map<Integer, List<Double>> timeMap, Map<Integer, List<Double>> displacementMap) {
        GPS startPoint = trajectory.get(0), endPoint = trajectory.get(trajectory.size() - 1), lastPoint = trajectory.get(0);
        double totalDistance = 0, totalTime = 0;
        for (GPS point : trajectory) {
            double displacement = CommonUtil.distanceBetween(point, startPoint);
            totalDistance += CommonUtil.distanceBetween(lastPoint, startPoint);
            totalTime += CommonUtil.timeBetween(lastPoint, startPoint);
            lastPoint = point;
            double endDisplacement = CommonUtil.distanceBetween(point, endPoint);

            int bucket = (int) (displacement / 100);
            if (!distanceMap.containsKey(bucket)) {
                distanceMap.put(bucket, new CopyOnWriteArrayList<>());
            }
            distanceMap.get(bucket).add(totalDistance);
            if (!timeMap.containsKey(bucket)) {
                timeMap.put(bucket, new CopyOnWriteArrayList<>());
            }
            timeMap.get(bucket).add(totalTime);
            if (!displacementMap.containsKey(bucket)) {
                displacementMap.put(bucket, new CopyOnWriteArrayList<>());
            }
            displacementMap.get(bucket).add(endDisplacement);
        }
    }

    private NormalDistribution gaussianModel(List<Double> list) {
        double[] array = list.stream().mapToDouble(x -> x).toArray();
        double mean = new Mean().evaluate(array);
        double std = new StandardDeviation().evaluate(array, mean);
        return new NormalDistribution(mean, std);
    }

    public double detect(List<List<GPS>> allNormalTrajectories, List<GPS> testTrajectory) {

        List<Double> linearDistance = new ArrayList<>();//x
        List<Double> intervals = new ArrayList<>();//y
        List<Double> totalDistance = new ArrayList<>();//y2

        Map<Integer, NormalDistribution> distanceModel, timeModel, displacementModel;

        Date startTime = testTrajectory.get(0).getTimestamp();
        Calendar calendar = GregorianCalendar.getInstance(); // creates a new calendar instance
        calendar.setTime(startTime);   // assigns calendar to given date
        int testStartHour = calendar.get(Calendar.HOUR_OF_DAY);

        // No cached.
        if (!cacheDisplacementModel.containsKey(testStartHour)) {

            //get LDT of each point
//            for (List<GPS> oneTrajectory : allNormalTrajectories) {
//                GPS startPoint = oneTrajectory.get(0);
//                GPS lastPoint = oneTrajectory.get(0);
//                drivingDist = 0;
//
//                //get start hour
//                calendar.setTime(startPoint.getTimestamp());
//                int currentStartHour = calendar.get(Calendar.HOUR_OF_DAY);
//
//                //filter
//                if (currentStartHour < testStartHour - 1 ||
//                        currentStartHour > testStartHour + 1)
//                    continue;
//
//                for (GPS currentPoint : oneTrajectory) {
//                    linearDist = CommonUtil.distanceBetween(startPoint, currentPoint);
//                    segment = CommonUtil.distanceBetween(lastPoint, currentPoint);
//                    drivingTime = (currentPoint.getTimestamp().getTime() - startPoint.getTimestamp().getTime()) / 1000;
//                    drivingDist += segment;
//                    linearDistance.add(linearDist);
//                    intervals.add(drivingTime);
//                    totalDistance.add(drivingDist);
//
//                    lastPoint = currentPoint;
//                }
//            }

            Map<Integer, List<Double>> distanceMap = new ConcurrentHashMap<>(), timeMap = new ConcurrentHashMap<>(), displacementMap = new ConcurrentHashMap<>();

            List<List<GPS>> filterTrajectory = filterByTime(allNormalTrajectories, testStartHour);
            //calculate the x,d,t
            filterTrajectory.forEach(x -> process(x, distanceMap, timeMap, displacementMap));
            //calculate model for each bucket
            distanceModel = Maps.transformValues(distanceMap, this::gaussianModel);
            timeModel = Maps.transformValues(timeMap, this::gaussianModel);
            displacementModel = Maps.transformValues(displacementMap, this::gaussianModel);

//            (linearDistance.size() == 0) {
//                System.out.println("no train set!");
//                return 1;
//            }
//
//            //Fitted the data set by maximum likelihood estimation
//            double[][] x = new double[linearDistance.size()][degree + 1];
//            for (int i = 0; i < linearDistance.size(); i++) {
//                for (int j = 0; j < degree + 1; j++) {
//                    x[i][j] = Math.pow(linearDistance.get(i), j);
//                }
//            }
//
//            double[] t = new double[intervals.size()];
//            double[] d = new double[totalDistance.size()];
//            for (int i = 0; i < intervals.size(); i++) {
//                t[i] = intervals.get(i);
//                d[i] = totalDistance.get(i);
//            }
//
//            RealMatrix X = MatrixUtils.createRealMatrix(x);
//            RealMatrix T = MatrixUtils.createColumnRealMatrix(t);
//            RealMatrix D = MatrixUtils.createColumnRealMatrix(d);
//            RealMatrix XT = X.transpose();
//            RealMatrix TT = T.transpose();
//            RealMatrix DT = D.transpose();
//            wt = MatrixUtils.inverse(XT.multiply(X)).multiply(XT).multiply(T).getColumn(0);
//            wd = MatrixUtils.inverse(XT.multiply(X)).multiply(XT).multiply(D).getColumn(0);
//            et = (TT.multiply(T).subtract(TT.multiply(X).multiply(MatrixUtils.createColumnRealMatrix(wt)))).getEntry(0, 0) / x.length;
//            ed = (DT.multiply(D).subtract(DT.multiply(X).multiply(MatrixUtils.createColumnRealMatrix(wd)))).getEntry(0, 0) / x.length;
            synchronized (STLOpDetection.class) {
                cacheDistanceModel.put(testStartHour, distanceModel);
                cacheTimeModel.put(testStartHour, timeModel);
                cacheDisplacementModel.put(testStartHour, displacementModel);
            }
        } else {
            synchronized (STLOpDetection.class) {
                distanceModel = cacheDistanceModel.get(testStartHour);
                timeModel = cacheTimeModel.get(testStartHour);
                displacementModel = cacheDisplacementModel.get(testStartHour);
            }
        }

//        PolynomialFunction polyT = new PolynomialFunction(wt);
//        PolynomialFunction polyD = new PolynomialFunction(wd);

        //detect the incoming trajectory
//        List<GPS> anomalyPoints = new ArrayList<>();
        double anomalyScore = 0;
        GPS startPos = testTrajectory.get(0), lastPos = testTrajectory.get(0), endPos = testTrajectory.get(testTrajectory.size() - 1);
        double drivingDist = 0, segment, drivingTime, endDisplacement;
        NormalDistribution curDisplacementModel, curDistanceModel, curTimeModel;
        double anomalyDisplacement, anomalyDistance, anomalyTime, meanAnomaly;
        int bucket;

        for (GPS gps : testTrajectory) {
            bucket = (int) (CommonUtil.distanceBetween(startPos, gps) / 100);
//            displacement = CommonUtil.distanceBetween(startPos, gps);
            segment = CommonUtil.distanceBetween(lastPos, gps);
            drivingTime = CommonUtil.timeBetween(gps, startPos);
            drivingDist += segment;
            endDisplacement = CommonUtil.distanceBetween(endPos, gps);

            curDisplacementModel = displacementModel.get(bucket);
            curDistanceModel = distanceModel.get(bucket);
            curTimeModel = timeModel.get(bucket);
            anomalyDisplacement = endDisplacement < curDisplacementModel.getMean() ? 0 : 1 - curDisplacementModel.density(endDisplacement) / curDisplacementModel.density(curDisplacementModel.getMean());
            anomalyDistance = drivingDist < curDistanceModel.getMean() ? 0 : 1 - curDistanceModel.density(drivingDist) / curDistanceModel.density(curDistanceModel.getMean());
            anomalyTime = drivingTime < curTimeModel.getMean() ? 0 : 1 - curTimeModel.density(drivingTime) / curTimeModel.density(curTimeModel.getMean());
            meanAnomaly = (anomalyDistance + anomalyDisplacement + anomalyTime) / 3;

//            double cdfT = new NormalDistribution(predictT, Math.sqrt(et)).cumulativeProbability(drivingTime);
//            double cdfD = new NormalDistribution(predictD, Math.sqrt(ed)).cumulativeProbability(drivingDist);
//
//            if (cdfT > thresholdTime && cdfD > thresholdDist)
//                anomalyPoints.add(gps);

            anomalyScore += segment * meanAnomaly;
            lastPos = gps;
        }

        return anomalyScore;
    }

}
