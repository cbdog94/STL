package algorithm;

import algorithm.cluster.ClusterUtil;
import algorithm.cluster.HierarchicalClustering;
import bean.GPS;
import org.apache.commons.math3.analysis.polynomials.PolynomialFunction;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import util.CommonUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Extension of STL algorithm
 *
 * @author Bin Cheng
 */
public class STLExtDetection {

    private final int DEGREE = 3;
    private final double THRESHOLD = 0.9;
    private Map<Integer, double[]> cacheWT = new ConcurrentHashMap<>();
    private Map<Integer, double[]> cacheWD = new ConcurrentHashMap<>();
    private Map<Integer, Double> cacheET = new ConcurrentHashMap<>();
    private Map<Integer, Double> cacheED = new ConcurrentHashMap<>();
    private List<List<GPS>> allNormalTrajectories;
    private int[] map;
    private List<List<Integer>> clusterHour;

    public STLExtDetection(List<List<GPS>> allNormalTrajectories, double t) {
        this.allNormalTrajectories = allNormalTrajectories;
        Map<Integer, Double> times = new ConcurrentHashMap<>();
        Map<Integer, Integer> counts = new ConcurrentHashMap<>();

        allNormalTrajectories.parallelStream().forEach(x -> {
            int startHour = CommonUtil.getHour(x.get(0));
            double time = CommonUtil.timeBetween(x.get(0), x.get(x.size() - 1));
            synchronized (this) {
                times.put(startHour, times.getOrDefault(startHour, 0.0) + time);
            }
            synchronized (this) {
                counts.put(startHour, counts.getOrDefault(startHour, 0) + 1);
            }
        });

        double[] timeList = new double[24];
        for (Map.Entry<Integer, Double> entry : times.entrySet()) {
            timeList[entry.getKey()] = entry.getValue() / counts.get(entry.getKey());
        }
        ClusterUtil clusterUtil = new HierarchicalClustering(timeList, t);
        map = clusterUtil.getClusterMap();
        clusterHour = clusterUtil.getAllClusters();
    }

    public double detect(List<GPS> testTrajectory) {
        return detect(testTrajectory, THRESHOLD, THRESHOLD, DEGREE);
    }

    public double detect(List<GPS> testTrajectory, double thresholdTime, double thresholdDist, int degree) {

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
        int cluster = map[testStartHour];

        if (!cacheWT.containsKey(cluster)) {

            //get LDT of each point
            for (List<GPS> oneTrajectory : allNormalTrajectories) {
                GPS startPoint = oneTrajectory.get(0);
                GPS lastPoint = oneTrajectory.get(0);
                drivingDist = 0;

                //get start hour
                calendar.setTime(startPoint.getTimestamp());
                int currentStartHour = calendar.get(Calendar.HOUR_OF_DAY);

                //filter
                if (!clusterHour.get(cluster).contains(currentStartHour))
                    continue;

                for (GPS currentPoint : oneTrajectory) {
                    linearDist = CommonUtil.distanceBetween(startPoint, currentPoint);
                    segment = CommonUtil.distanceBetween(lastPoint, currentPoint);
                    drivingTime = CommonUtil.timeBetween(startPoint, currentPoint);
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
            synchronized (this) {
                cacheWT.put(cluster, wt);
                cacheWD.put(cluster, wd);
                cacheET.put(cluster, et);
                cacheED.put(cluster, ed);
            }
        } else {
            synchronized (this) {
                wt = cacheWT.get(cluster);
                wd = cacheWD.get(cluster);
                et = cacheET.get(cluster);
                ed = cacheED.get(cluster);
            }
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
            drivingTime = CommonUtil.timeBetween(gps, startPos);
            drivingDist += segment;
            double predictT = polyT.value(linearDist);
            double predictD = polyD.value(linearDist);

            double cdfT = new NormalDistribution(predictT, Math.sqrt(et)).cumulativeProbability(drivingTime);
            double cdfD = new NormalDistribution(predictD, Math.sqrt(ed)).cumulativeProbability(drivingDist);

            double anomalyT = drivingTime < predictT ? 0 : 1 - new NormalDistribution(predictT, Math.sqrt(et)).density(drivingTime) / new NormalDistribution(predictT, Math.sqrt(et)).density(predictT);
            double anomalyD = drivingDist < predictD ? 0 : 1 - new NormalDistribution(predictD, Math.sqrt(ed)).density(drivingDist) / new NormalDistribution(predictD, Math.sqrt(ed)).density(predictD);
//            System.out.println(anomalyT + " " + anomalyD);
//            System.out.println(Math.min(anomalyT, anomalyD));
//            System.out.println("-----------");

            if (cdfT > thresholdTime && cdfD > thresholdDist)
                anomalyPoints.add(gps);

//            anomalyScore += segment / (1 + Math.exp(100 * Math.max(thresholdTime - cdfT, thresholdDist - cdfD)));
            anomalyScore += segment * Math.min(anomalyT, anomalyD);
            lastPos = gps;
        }
//        System.out.println(anomalyScore);
        return anomalyScore;
    }

//    public static void main(String[] args) {
//        double[] list = new double[]{449, 271, 139, 60, 50, 99, 178, 315, 610, 633, 499, 436, 486, 579, 551, 579, 509, 540, 521, 475, 480, 573, 626, 527};
//        ClusterUtil clusterUtil = new KMeans(list);
//        System.out.println(Arrays.toString(clusterUtil.getClusterMap()));
//        System.out.pristerUtil.getAllClusters());
//    }


}
