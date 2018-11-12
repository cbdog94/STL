package algorithm;

import bean.GPS;
import com.google.common.collect.Maps;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import util.CommonUtil;

import java.io.Serializable;
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

    private static Map<Integer, Map<Integer, NormalDistribution>> cacheDisplacementModel = new ConcurrentHashMap<>();
    private static Map<Integer, Map<Integer, NormalDistribution>> cacheDistanceModel = new ConcurrentHashMap<>();
    private static Map<Integer, Map<Integer, NormalDistribution>> cacheTimeModel = new ConcurrentHashMap<>();

    private List<List<GPS>> filterByTime(List<List<GPS>> allNormalTrajectories, int startHour) {
        Calendar calendar = GregorianCalendar.getInstance();
        return allNormalTrajectories.stream().filter(x -> {
            calendar.setTime(x.get(0).getTimestamp());
            int currentStartHour = calendar.get(Calendar.HOUR_OF_DAY);
            return currentStartHour >= startHour - 1 &&
                    currentStartHour <= startHour + 1;
        }).collect(Collectors.toList());
    }

    private void process(List<GPS> trajectory, Map<Integer, List<Double>> distanceMap, Map<Integer, List<Double>> timeMap, Map<Integer, List<Double>> displacementMap) {
        GPS startPoint = trajectory.get(0), endPoint = trajectory.get(trajectory.size() - 1), lastPoint = trajectory.get(0);
        double totalDistance = 0, totalTime = 0;
        for (GPS point : trajectory) {
            double displacement = CommonUtil.distanceBetween(point, startPoint);
            totalDistance += CommonUtil.distanceBetween(lastPoint, point);
            totalTime += CommonUtil.timeBetween(lastPoint, point);
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
        if (std == 0) {
            return null;
        }
        return new NormalDistribution(mean, std);
    }

    public double detect(List<List<GPS>> allNormalTrajectories, List<GPS> testTrajectory) {
        return detect(allNormalTrajectories, testTrajectory, new ArrayList<>(testTrajectory.size()), false);
    }

    public double detect(List<List<GPS>> allNormalTrajectories, List<GPS> testTrajectory, List<AnomalyInfo> anomalyInfoList, boolean debug) {

        Map<Integer, NormalDistribution> distanceModel, timeModel, displacementModel;

        Date startTime = testTrajectory.get(0).getTimestamp();
        Calendar calendar = GregorianCalendar.getInstance(); // creates a new calendar instance
        calendar.setTime(startTime);   // assigns calendar to given date
        int testStartHour = calendar.get(Calendar.HOUR_OF_DAY);

        // No cached.
        if (!cacheDisplacementModel.containsKey(testStartHour)) {
            Map<Integer, List<Double>> distanceMap = new ConcurrentHashMap<>(), timeMap = new ConcurrentHashMap<>(), displacementMap = new ConcurrentHashMap<>();
            List<List<GPS>> filterTrajectory = filterByTime(allNormalTrajectories, testStartHour);
            //calculate the x,d,t
            filterTrajectory.forEach(x -> process(x, distanceMap, timeMap, displacementMap));
            //calculate model for each bucket
            distanceModel = Maps.transformValues(distanceMap, this::gaussianModel);
            timeModel = Maps.transformValues(timeMap, this::gaussianModel);
            displacementModel = Maps.transformValues(displacementMap, this::gaussianModel);

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
            if (curDisplacementModel != null && curDistanceModel != null && curTimeModel != null) {
                anomalyDisplacement = endDisplacement < curDisplacementModel.getMean() ? 0 : 1 - curDisplacementModel.density(endDisplacement) / curDisplacementModel.density(curDisplacementModel.getMean());
                anomalyDistance = drivingDist < curDistanceModel.getMean() ? 0 : 1 - curDistanceModel.density(drivingDist) / curDistanceModel.density(curDistanceModel.getMean());
                anomalyTime = drivingTime < curTimeModel.getMean() ? 0 : 1 - curTimeModel.density(drivingTime) / curTimeModel.density(curTimeModel.getMean());
                meanAnomaly = (Math.min(anomalyDistance, anomalyTime) + anomalyDisplacement) / 2;
                anomalyScore += segment * meanAnomaly;
                anomalyInfoList.add(new AnomalyInfo(gps, anomalyDisplacement, anomalyDistance, anomalyTime, segment * meanAnomaly));
                if (debug) {
                    System.out.println(bucket);
                    System.out.println("Displacement Model:");
                    System.out.println("Mean:" + curDisplacementModel.getMean() + " Std:" + curDisplacementModel.getStandardDeviation());
                    System.out.println("Cur:" + endDisplacement);
                    System.out.println("Distance Model:");
                    System.out.println("Mean:" + curDistanceModel.getMean() + " Std:" + curDistanceModel.getStandardDeviation());
                    System.out.println("Cur:" + drivingDist);
                    System.out.println("Time Model:");
                    System.out.println("Mean:" + curTimeModel.getMean() + " Std:" + curTimeModel.getStandardDeviation());
                    System.out.println("Cur:" + drivingTime);
                    System.out.println();
                }
            }

            lastPos = gps;
        }
        return anomalyScore;
    }

    public static class AnomalyInfo implements Serializable {
        double latitude;
        double longitude;
        Date timestamp;
        double anomalyDisplacement;
        double anomalyDistance;
        double anomalyTime;
        double anomalyScore;

        AnomalyInfo(GPS gps, double anomalyDisplacement, double anomalyDistance, double anomalyTime, double anomalyScore) {
            this.latitude = gps.getLatitude();
            this.longitude = gps.getLongitude();
            this.timestamp = gps.getTimestamp();
            this.anomalyDisplacement = anomalyDisplacement;
            this.anomalyDistance = anomalyDistance;
            this.anomalyTime = anomalyTime;
            this.anomalyScore = anomalyScore;
        }
    }

}
