package algorithm;

import bean.Cell;
import bean.GPS;
import com.google.gson.Gson;
import hbase.TrajectoryUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.analysis.polynomials.PolynomialFunction;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import util.CommonUtil;
import util.TileSystem;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * The algorithm of iBOAT.
 *
 * @author Bin Cheng
 */
public class CombinationDetection {

    private static double threshold_iBOAT = 0.04;
    private static double threshold_DTM = 0.8;
    private static int degree = 3;

    private static List<Cell> adaptiveWindow;//= new ArrayList<>();
    private static GPS lastGPS;
    private static GPS endGPS;

    /**
     * 联合检测算法
     *
     * @return 异常点占整个轨迹的百分比
     */
    public static double combination(String id, Cell startPoint, Cell endPoint) {

        //得到待测ID的GPS轨迹
        List<GPS> testTrajectory = TrajectoryUtil.getTrajectoryGPSPoints(id);
        testTrajectory = CommonUtil.removeExtraGPS(testTrajectory, startPoint, endPoint);

        //起止点的所有网格轨迹
        Map<String, List<Cell>> allTrajectories = TrajectoryUtil.getAllTrajectoryCells(startPoint, endPoint);
        allTrajectories.remove(id);
        System.out.println(allTrajectories.size());

        Map<String, Double> distance = new HashMap<>();
        Map<String, Double> time = new HashMap<>();//min

        //计算每条轨迹所用的距离和时间
        for (String eachID : allTrajectories.keySet()) {
            List<GPS> trajectory = TrajectoryUtil.getTrajectoryGPSPoints(eachID);
            trajectory = CommonUtil.removeExtraGPS(trajectory, startPoint, endPoint);
            if (trajectory == null || trajectory.size() == 0) {
                continue;
            }

            distance.put(eachID, CommonUtil.trajectoryDistance(trajectory));
            long endTime = trajectory.get(trajectory.size() - 1).getTimestamp().getTime();
            long startTime = trajectory.get(0).getTimestamp().getTime();

            time.put(eachID, (endTime - startTime) * 1.0 / (60 * 1000));
        }

        System.out.println(distance.size() + " " + time.size());

        double distance60 = CommonUtil.percentile(distance.values().toArray(new Double[distance.size()]), 0.6);
        double time60 = CommonUtil.percentile(time.values().toArray(new Double[time.size()]), 0.6);
        System.out.println("median distance:" + CommonUtil.percentile(distance.values().toArray(new Double[distance.size()]), 0.5));
        System.out.println("median time:" + CommonUtil.percentile(time.values().toArray(new Double[time.size()]), 0.5));
        //得到DTM需要的训练集ID
        List<String> DTMTrainID = new ArrayList<>();
        for (String ID : distance.keySet()) {
            if (distance.get(ID) < distance60 || time.get(ID) < time60)
                DTMTrainID.add(ID);
        }

        System.out.println("train size:" + DTMTrainID.size());

        List<Double> distances = new ArrayList<>();//x
        List<Double> intervals = new ArrayList<>();//y
        List<Double> totalDistance = new ArrayList<>();//y2

        Date startTime = testTrajectory.get(0).getTimestamp();
        Date endTime = testTrajectory.get(testTrajectory.size() - 1).getTimestamp();

        System.out.println("total time: " + (endTime.getTime() - startTime.getTime()) / 1000);
        System.out.println("total distance: " + CommonUtil.trajectoryDistance(testTrajectory));

        //得到DTM需要的训练集
        for (String trajectoryID : DTMTrainID) {
            List<GPS> trajectory = TrajectoryUtil.getTrajectoryGPSPoints(trajectoryID);
            trajectory = CommonUtil.removeExtraGPS(trajectory, startPoint, endPoint);
            if (trajectory == null || trajectory.size() == 0) {
                continue;
            }

            GPS start = trajectory.get(0);
            GPS lastPoint = trajectory.get(0);
            double dist = 0;
//            System.out.println(start.getTimestamp());
            if (trajectory.get(0).getTimestamp().getHours() < startTime.getHours() - 1 ||
                    trajectory.get(0).getTimestamp().getHours() > startTime.getHours() + 1)
                continue;
            for (GPS aTrajectory : trajectory) {
                double dis = CommonUtil.distanceBetween(start, aTrajectory);
                double segment = CommonUtil.distanceBetween(lastPoint, aTrajectory);
                double interval = (aTrajectory.getTimestamp().getTime() - start.getTimestamp().getTime()) / 1000;
                dist += segment;
                if (interval < 0) {
                    System.out.println("interval<0");
                    break;
                }
                totalDistance.add(dist);
                distances.add(dis);
                intervals.add(interval);
                lastPoint = aTrajectory;
            }
        }

        if (distances.size() == 0)
            System.out.println("distances.size()");

        //train
        double[][] x = new double[distances.size()][degree + 1];
        for (int i = 0; i < distances.size(); i++) {
            for (int j = 0; j < degree + 1; j++) {
                x[i][j] = Math.pow(distances.get(i), j);
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
        double[] wt = MatrixUtils.inverse(XT.multiply(X)).multiply(XT).multiply(T).getColumn(0);
        double[] wd = MatrixUtils.inverse(XT.multiply(X)).multiply(XT).multiply(D).getColumn(0);
        double et = (TT.multiply(T).subtract(TT.multiply(X).multiply(MatrixUtils.createColumnRealMatrix(wt)))).getEntry(0, 0) / x.length;
        double ed = (DT.multiply(D).subtract(DT.multiply(X).multiply(MatrixUtils.createColumnRealMatrix(wd)))).getEntry(0, 0) / x.length;

        PolynomialFunction polyT = new PolynomialFunction(wt);
        PolynomialFunction polyD = new PolynomialFunction(wd);

        List<Integer> anomaly = new ArrayList<>();
        List<List<Cell>> supportTrajectories = new ArrayList<>(allTrajectories.values());
        adaptiveWindow = new ArrayList<>();
        int lastPosition = 0;
        double score = 0.0;
        int cellNum = 0;

        GPS startPos = testTrajectory.get(0);
        double dist = 0;

        List<PointInfo> pointInfos = new ArrayList<>();

        for (int i = 0; i < testTrajectory.size(); i++) {
            boolean flag = false;
            Cell cell = TileSystem.GPSToTile(testTrajectory.get(i));
            if (cell.equals(TileSystem.GPSToTile(testTrajectory.get(lastPosition))))
                continue;
            cellNum++;

            adaptiveWindow.add(cell);

            int lastSupportSize = supportTrajectories.size();
            supportTrajectories.removeIf(cells -> !hasCommonPath(adaptiveWindow, cells));

            double support = supportTrajectories.size() * 1.0 / lastSupportSize;
            double segment = CommonUtil.distanceBetween(testTrajectory.get(lastPosition), testTrajectory.get(i));
//            System.out.println("support: " + support);
            dist += segment;

            //LDTM detect
            GPS gps = testTrajectory.get(i);
            double dis = CommonUtil.distanceBetween(startPos, gps);
            double interval = (gps.getTimestamp().getTime() - startTime.getTime()) / 1000;

            double predictT = polyT.value(dis);
            double predictD = polyD.value(dis);

            NormalDistribution distributionT = new NormalDistribution(predictT, Math.sqrt(et));
            NormalDistribution distributionD = new NormalDistribution(predictD, Math.sqrt(ed));

            double cdfT = distributionT.cumulativeProbability(interval);
            double cdfD = distributionD.cumulativeProbability(dist);
            System.out.println("cdfT:" + cdfT);
            System.out.println("cdfD:" + cdfD);

            if (support < threshold_iBOAT) {
                System.out.println("iBOAT think anomalous");
                if (cdfT > threshold_DTM && cdfD > threshold_DTM) {
                    anomaly.add(i);
                    System.out.println(i + " is anomalous!");
                    flag = true;
                }

                score += scoreForDTM(Math.min(cdfT, cdfD), threshold_DTM, segment);
//                score += scoreForIBOAT(support, threshold_iBOAT, dist);
                supportTrajectories = new ArrayList<>(allTrajectories.values());
                adaptiveWindow.clear();
                adaptiveWindow.add(cell);
            } else {
                score += scoreForIBOAT(support, threshold_iBOAT, segment);
            }

            pointInfos.add(new PointInfo(testTrajectory.get(i).getLatitude(), testTrajectory.get(i).getLongitude(),
                    testTrajectory.get(i).getTimestamp(), flag, score));
            System.out.println(score);
            lastPosition = i;
        }
        File file = FileUtils.getFile("testResult.json");
        try {
            FileUtils.write(file, new Gson().toJson(pointInfos));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        return 0;
//        return anomalyCells.size() * 1.0 / cellNum;
    }


    private static boolean hasCommonPath(List<Cell> testPath, List<Cell> samplePath) {
        int index = -1;
        boolean search;
        for (Cell cell : testPath) {
            search = false;
            for (int i = index + 1; i < samplePath.size(); i++) {
                if (samplePath.get(i).equals(cell)) {
                    index = i;
                    search = true;
                    break;
                }
            }
            if (!search)
                return false;
        }
        return true;
    }

    private static double scoreForIBOAT(double support, double threshold, double distance) {
        int lambda = 150;
        double t = 1 + Math.exp(lambda * (support - threshold));
        return distance / t;
    }

    private static double scoreForDTM(double support, double threshold, double distance) {
        int lambda = 200;
        double t = 1 + Math.exp(lambda * (threshold - support));
        return distance / t;
    }

    public static class PointInfo {
        double latitude;
        double longitude;
        Date timestamp;
        boolean anomaly;
        double score1;

        public PointInfo(double latitude, double longitude, Date timestamp, boolean anomaly, double score1) {
            this.latitude = latitude;
            this.longitude = longitude;
            this.timestamp = timestamp;
            this.anomaly = anomaly;
            this.score1 = score1;
        }
    }
}
