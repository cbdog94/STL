package prepare;

import bean.Cell;
import bean.GPS;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import hbase.TrajectoryUtil;
import org.apache.commons.io.FileUtils;
import util.CommonUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by cbdog94 on 17-3-17.
 */
public class SelectTrajectories {

    private Map<Integer, List<Double>> normalDistanceMap = new ConcurrentHashMap<>(), anomalyDistanceMap = new ConcurrentHashMap<>();
    private Map<Integer, List<Double>> normalTimeMap = new ConcurrentHashMap<>(), anomalyTimeMap = new ConcurrentHashMap<>();
    private Map<Integer, List<Double>> normalDisplacementMap = new ConcurrentHashMap<>(), anomalyDisplacementMap = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException {
        Cell startPoint, endPoint;

        if (args.length < 2) {
//         startPoint = new Cell("[219505,107119]");//延安路华山路(静安寺)
            //Level 18
//            startPoint = new Cell("[219552,107108]");//陆家嘴
//            endPoint = new Cell("[219747,107149]");//浦东机场

            //Level 17
            startPoint = new Cell("[109776,53554]");//陆家嘴
            endPoint = new Cell("[109873,53574]");//浦东机场

        } else {
            startPoint = new Cell(args[0]);
            endPoint = new Cell(args[1]);
        }

        new SelectTrajectories().printAllTrajectoryPoints(startPoint, endPoint);

    }

    private void printAllTrajectoryPoints(Cell start, Cell end) {
        Map<String, List<GPS>> trajectories = TrajectoryUtil.getAllTrajectoryGPSs(start, end, "SH");
        System.out.println(trajectories.size());
        // Anomaly Label.
        Set<String> anomalyTrajectory = CommonUtil.anomalyTrajectory(trajectories, "SH", true);

        Map<String, List<GPS>> normalTrajectories = Maps.filterKeys(trajectories, (x) -> !anomalyTrajectory.contains(x));
        Map<String, List<GPS>> anomalyTrajectories = Maps.filterKeys(trajectories, (x) -> anomalyTrajectory.contains(x));

        normalTrajectories.forEach((x, y) -> process(y, normalDistanceMap, normalTimeMap, normalDisplacementMap));
        anomalyTrajectories.forEach((x, y) -> process(y, anomalyDistanceMap, anomalyTimeMap, anomalyDisplacementMap));

        try {
            FileUtils.write(FileUtils.getFile("normalDistanceMap.json"), new Gson().toJson(normalDistanceMap), "UTF-8");
            FileUtils.write(FileUtils.getFile("normalTimeMap.json"), new Gson().toJson(normalTimeMap), "UTF-8");
            FileUtils.write(FileUtils.getFile("anomalyDistanceMap.json"), new Gson().toJson(anomalyDistanceMap), "UTF-8");
            FileUtils.write(FileUtils.getFile("anomalyTimeMap.json"), new Gson().toJson(anomalyTimeMap), "UTF-8");
            FileUtils.write(FileUtils.getFile("normalDisplacementMap.json"), new Gson().toJson(normalDisplacementMap), "UTF-8");
            FileUtils.write(FileUtils.getFile("anomalyDisplacementMap.json"), new Gson().toJson(anomalyDisplacementMap), "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }

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


}
