package algorithm;

import bean.Cell;
import bean.GPS;
import util.CommonUtil;
import util.TileSystem;
import web.OnlineDetect;

import java.util.ArrayList;
import java.util.List;

/**
 * The algorithm of iBOAT.
 *
 * @author Bin Cheng
 */
public class iBOATDetection {

    private static double threshold = 0.04;

    public static double iBOAT(List<GPS> testTrajectory, List<List<Cell>> allTrajectories) {
        return iBOAT(testTrajectory, allTrajectories, threshold);
    }

    /**
     * The offline implementation of iBOAT.
     *
     * @param testTrajectory  Testing trajectory
     * @param allTrajectories All historical trajectories
     * @param threshold       threshold of support ratio
     * @return the proportion of anomalous points
     */
    public static double iBOAT(List<GPS> testTrajectory, List<List<Cell>> allTrajectories, double threshold) {
        List<Cell> anomalyCells = new ArrayList<>();
        List<List<Cell>> supportTrajectories = new ArrayList<>(allTrajectories);
        List<Cell> adaptiveWindow = new ArrayList<>();
        int lastPosition = 0;
        double score = 0.0;
        int cellNum = 0;

        for (int i = 0; i < testTrajectory.size(); i++) {
            Cell cell = TileSystem.gpsToTile(testTrajectory.get(i));
            if (TileSystem.equal(cell,TileSystem.gpsToTile(testTrajectory.get(lastPosition)))) {
                continue;
            }
            cellNum++;

            adaptiveWindow.add(cell);

            int lastSupportSize = supportTrajectories.size();
            supportTrajectories.removeIf(cells -> !hasCommonPath(adaptiveWindow, cells));

            double support = supportTrajectories.size() * 1.0 / lastSupportSize;
            if (support < threshold) {
                anomalyCells.add(cell);
                supportTrajectories = new ArrayList<>(allTrajectories);
                adaptiveWindow.clear();
                adaptiveWindow.add(cell);
            }

            double distance = CommonUtil.distanceBetween(testTrajectory.get(lastPosition), testTrajectory.get(i));
            score += scoreAccumulation(support, threshold, distance);
            lastPosition = i;
        }
        return anomalyCells.size() * 1.0 / cellNum;
    }

    private static List<Cell> anomalyCells;
    private static List<List<Cell>> supportTrajectories;
    private static List<Cell> adaptiveWindow;
    private static List<List<Cell>> allTrajectories;
    private static double score = 0.0;
    private static GPS lastGPS;
    private static GPS endGPS;

    private static final int NORMAL = 0;
    private static final int ANOMALY = 1;
    private static final int IGNORE = 2;
    private static final int FINISHED = 3;

    /**
     * @return 0 normal, 1 anomaly, 2 ignore, 3 finished
     */
    public synchronized static DetectResult iBOATOnline(String id, GPS gps) {

        DetectResult result = new DetectResult();

        epilog(id);

        Cell cell = TileSystem.gpsToTile(gps);

        if (TileSystem.equal(cell,TileSystem.gpsToTile(endGPS))) {
            result.code = FINISHED;
            return result;
        } else if (TileSystem.equal(cell,TileSystem.gpsToTile(lastGPS))) {
            result.code = IGNORE;
            return result;
        }

        boolean anomaly = false;

        adaptiveWindow.add(cell);

        int lastSupportSize = supportTrajectories.size();
        supportTrajectories.removeIf(cells -> !hasCommonPath(adaptiveWindow, cells));

        System.out.println("--------");
        System.out.println(cell);
        System.out.println(lastSupportSize + " " + supportTrajectories.size());

        double support = supportTrajectories.size() * 1.0 / lastSupportSize;
        if (support < threshold) {
            anomalyCells.add(cell);
            supportTrajectories = new ArrayList<>(allTrajectories);
            adaptiveWindow.clear();
            adaptiveWindow.add(cell);
            anomaly = true;
        }

        double distance = CommonUtil.distanceBetween(lastGPS, gps);
        score += scoreAccumulation(support, threshold, distance);

        lastGPS = gps;
        prolog(id);

        result.code = anomaly ? ANOMALY : NORMAL;
        result.lastSupportSize = lastSupportSize;
        result.currentSupportSize = supportTrajectories.size();
        result.support = support;
        return result;

    }

    /**
     * 恢复现场
     */
    private static void epilog(String id) {
        anomalyCells = OnlineDetect.anomalyCells.get(id);
        supportTrajectories = OnlineDetect.supportTrajectories.get(id);
        adaptiveWindow = OnlineDetect.adaptiveWindow.get(id);
        allTrajectories = OnlineDetect.allTrajectories.get(id);
        score = OnlineDetect.score.get(id);
        lastGPS = OnlineDetect.lastGPS.get(id);
        endGPS = OnlineDetect.ends.get(id);
    }

    /**
     * 保护现场
     */
    private static void prolog(String id) {
        OnlineDetect.anomalyCells.put(id, anomalyCells);
        OnlineDetect.supportTrajectories.put(id, supportTrajectories);
        OnlineDetect.adaptiveWindow.put(id, adaptiveWindow);
        OnlineDetect.score.put(id, score);
        OnlineDetect.lastGPS.put(id, lastGPS);
    }

    private static boolean hasCommonPath(List<Cell> testPath, List<Cell> samplePath) {
        int index = -1;
        boolean search;
        for (Cell cell : testPath) {
            search = false;
            for (int i = index + 1; i < samplePath.size(); i++) {
                if (TileSystem.equal(samplePath.get(i),cell)) {
                    index = i;
                    search = true;
                    break;
                }
            }
            if (!search) {
                return false;
            }
        }
        return true;
    }

    private static double scoreAccumulation(double support, double threshold, double distance) {
        int lambda = 150;
        double t = 1 + Math.exp(lambda * (support - threshold));
        return distance / t;
    }

    public static class DetectResult {
        public int code;
        public int lastSupportSize;
        public int currentSupportSize;
        public double support;
    }
}
