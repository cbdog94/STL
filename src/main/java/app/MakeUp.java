package app;

import algorithm.STLDetection;
import algorithm.iBOATDetection;
import bean.Cell;
import bean.GPS;
import constant.DetectConstant;
import hbase.TrajectoryUtil;
import util.CommonUtil;

import java.io.IOException;
import java.util.*;

/**
 * Distance-Time Model
 */
public class MakeUp {


    public static void main(String[] args) throws IOException {

        Cell startPoint = DetectConstant.startPoint;
        Cell endPoint = DetectConstant.endPoint;

        if (args.length == 1) {
            Cell[] cells = DetectConstant.getCellPoint(Integer.parseInt(args[4]));
            startPoint = cells[0];//陆家嘴
            endPoint = cells[1];//浦东机场
        }

        double threshold = 0.8;
        int degree = 3;
        double quantile = 0.6;

        //test set
        Map<String, List<Cell>> testTrajectories = TrajectoryUtil.getAllTrajectoryCells(startPoint, endPoint);

        List<Cell> startCells = new ArrayList<>();
        List<Cell> endCells = new ArrayList<>();

        startCells.add(new Cell(startPoint.getTileX() - 1, startPoint.getTileY() - 1));
        startCells.add(new Cell(startPoint.getTileX() - 1, startPoint.getTileY()));
        startCells.add(new Cell(startPoint.getTileX() - 1, startPoint.getTileY() + 1));
        startCells.add(new Cell(startPoint.getTileX() + 1, startPoint.getTileY() - 1));
        startCells.add(new Cell(startPoint.getTileX() + 1, startPoint.getTileY()));
        startCells.add(new Cell(startPoint.getTileX() + 1, startPoint.getTileY() + 1));
        startCells.add(new Cell(startPoint.getTileX(), startPoint.getTileY() - 1));
        startCells.add(new Cell(startPoint.getTileX(), startPoint.getTileY() + 1));

        endCells.add(new Cell(endPoint.getTileX() - 1, endPoint.getTileY() - 1));
        endCells.add(new Cell(endPoint.getTileX() - 1, endPoint.getTileY()));
        endCells.add(new Cell(endPoint.getTileX() - 1, endPoint.getTileY() + 1));
        endCells.add(new Cell(endPoint.getTileX() + 1, endPoint.getTileY() - 1));
        endCells.add(new Cell(endPoint.getTileX() + 1, endPoint.getTileY()));
        endCells.add(new Cell(endPoint.getTileX() + 1, endPoint.getTileY() + 1));
        endCells.add(new Cell(endPoint.getTileX(), endPoint.getTileY() - 1));
        endCells.add(new Cell(endPoint.getTileX(), endPoint.getTileY() + 1));

        //train set
        Map<String, List<Cell>> allTrajectories = new HashMap<>();
        Map<String, List<GPS>> allTrajectoriesGPS = new HashMap<>();
        for (Cell start : startCells)
            for (Cell end : endCells) {
                Map<String, List<Cell>> tmpTrajectories = TrajectoryUtil.getAllTrajectoryCells(start, end);
                allTrajectories.putAll(tmpTrajectories);
                System.out.println(allTrajectories.size());
                for (String id : tmpTrajectories.keySet()) {
                    if (testTrajectories.containsKey(id))
                        continue;
                    List<GPS> gpsTra = TrajectoryUtil.getTrajectoryGPSPoints(id);
                    gpsTra = CommonUtil.removeExtraGPS(gpsTra, start, end);
                    if (gpsTra == null || gpsTra.size() == 0) {
                        continue;
                    }
                    allTrajectoriesGPS.put(id, gpsTra);
                }
            }
        for (String id : testTrajectories.keySet()) {
            allTrajectories.remove(id);
        }
        System.out.println("all size:" + allTrajectories.size());
        System.out.println("all size:" + allTrajectoriesGPS.size());
        System.out.println("--------------------------");

        Set<String> iBOATSetN = new HashSet<>();
        Set<String> iBOATSetA = new HashSet<>();

        Map<String, Double> distance = new HashMap<>();
        Map<String, Double> time = new HashMap<>();//min

        long totalIBOAT = 0;

        for (String id : testTrajectories.keySet()) {

            List<GPS> testTrajectory = TrajectoryUtil.getTrajectoryGPSPoints(id);
            testTrajectory = CommonUtil.removeExtraGPS(testTrajectory, startPoint, endPoint);
            if (testTrajectory == null || testTrajectory.size() == 0) {
                continue;
            }

            distance.put(id, CommonUtil.trajectoryDistance(testTrajectory));
            long endTime = testTrajectory.get(testTrajectory.size() - 1).getTimestamp().getTime();
            long startTime = testTrajectory.get(0).getTimestamp().getTime();

            time.put(id, (endTime - startTime) * 1.0 / (60 * 1000));

//            Map<String, List<Cell>> tmpTrajectories = new HashMap<>(allTrajectories);
//            tmpTrajectories.remove(id);

            long start = System.currentTimeMillis();
            double result = iBOATDetection.iBOAT(testTrajectory, new ArrayList<>(allTrajectories.values()));
            long end = System.currentTimeMillis();

            totalIBOAT += end - start;

            if (result < 0.1)
                iBOATSetN.add(id);
            else
                iBOATSetA.add(id);
        }

        System.out.println("TotalTime:" + totalIBOAT);
        double perTime = totalIBOAT * 1.0 / (iBOATSetN.size() + iBOATSetA.size());
        System.out.println("PerTime:" + perTime);
        System.out.println("finish iBOAT!");

        System.out.println("iBOAT Normal size:" + iBOATSetN.size());
        System.out.println("iBOAT Anomaly size:" + iBOATSetA.size());

        double distance90 = CommonUtil.percentile(distance.values().toArray(new Double[distance.size()]), 0.85);
        double time90 = CommonUtil.percentile(time.values().toArray(new Double[time.size()]), 0.85);


        List<String> anomaly = new ArrayList<>();
        for (String id : distance.keySet()) {
            if (distance.get(id) > distance90 && time.get(id) > time90)
                anomaly.add(id);
        }

        Set<String> preAnomaly = new HashSet<>(iBOATSetA);
        Set<String> preNormal = new HashSet<>(iBOATSetN);
        int allAnomaly = preAnomaly.size();
        preAnomaly.retainAll(anomaly);
        System.out.println("TP:" + preAnomaly.size());
        System.out.println("FP:" + (allAnomaly - preAnomaly.size()));

        int allNormal = preNormal.size();
        preNormal.retainAll(anomaly);

        System.out.println("FN:" + preNormal.size());
        System.out.println("TN:" + (allNormal - preNormal.size()));

        int TP = preAnomaly.size();
        int FP = allAnomaly - preAnomaly.size();
        int FN = preNormal.size();
        int TN = allNormal - preNormal.size();

        System.out.println("TPR:" + TP * 1.0 / (TP + FN));
        System.out.println("FPR:" + FP * 1.0 / (FP + TN));

        System.out.println("---------------");

//DTM
        distance.clear();
        time.clear();

        for (String id : allTrajectoriesGPS.keySet()) {

            List<GPS> gpsTrajectory = allTrajectoriesGPS.get(id);

            distance.put(id, CommonUtil.trajectoryDistance(gpsTrajectory));
            long endTime = gpsTrajectory.get(gpsTrajectory.size() - 1).getTimestamp().getTime();
            long startTime = gpsTrajectory.get(0).getTimestamp().getTime();

            time.put(id, (endTime - startTime) * 1.0 / (60 * 1000));

        }

        double distance60 = CommonUtil.percentile(distance.values().toArray(new Double[distance.size()]), quantile);
        double time60 = CommonUtil.percentile(time.values().toArray(new Double[time.size()]), quantile);


        List<List<GPS>> allList = new ArrayList<>();
        for (String id : distance.keySet()) {
            if (distance.get(id) < distance60 || time.get(id) < time60) {
                allList.add(allTrajectoriesGPS.get(id));
            }
        }

        System.out.println("train set size:" + allList.size());


        //Only DTM
        long totalDTM = 0;
        Set<String> DTMAnomaly = new HashSet<>();
        Set<String> DTMNormal = new HashSet<>();
        for (String trajectoryID : testTrajectories.keySet()) {

            List<GPS> trajectory = TrajectoryUtil.getTrajectoryGPSPoints(trajectoryID);
            trajectory = CommonUtil.removeExtraGPS(trajectory, startPoint, endPoint);
            if (trajectory == null || trajectory.size() == 0) {
                continue;
            }

            long start = System.currentTimeMillis();
            double score = STLDetection.detect(allList, trajectory, threshold, threshold, degree);
            long end = System.currentTimeMillis();
            totalDTM += end - start;

            if (score < 0.1)
                DTMNormal.add(trajectoryID);
            else
                DTMAnomaly.add(trajectoryID);
        }

        allAnomaly = DTMAnomaly.size();
        allNormal = DTMNormal.size();

        DTMAnomaly.retainAll(anomaly);
        DTMNormal.retainAll(anomaly);
        TP = DTMAnomaly.size();
        FP = allAnomaly - DTMAnomaly.size();
        FN = DTMNormal.size();
        TN = allNormal - DTMNormal.size();
        System.out.println("DTM-----------");
        System.out.println("TP:" + TP);
        System.out.println("FP:" + FP);

        System.out.println("FN:" + FN);
        System.out.println("TN:" + TN);


        System.out.println("TPR:" + TP * 1.0 / (TP + FN));
        System.out.println("FPR:" + FP * 1.0 / (FP + TN));
    }


}
