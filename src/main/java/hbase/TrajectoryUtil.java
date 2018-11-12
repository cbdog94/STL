package hbase;

import bean.Cell;
import bean.GPS;
import constant.HBaseConstant;
import util.CommonUtil;
import util.TileSystem;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The methods of getting trajectory.
 *
 * @author Bin Cheng
 */
public class TrajectoryUtil {

    private static HBaseUtil hBaseUtil = new HBaseUtil();

    /**
     * Acquire the trajectoryPoints of trajectoryID from HBase
     */
    public static List<GPS> getTrajectoryGPSPoints(String trajectoryID) {
        String result = hBaseUtil.getColumnFromHBase(
                HBaseConstant.TABLE_SH_TRAJECTORY,
                trajectoryID.getBytes(),
                HBaseConstant.COLUMN_FAMILY_TRAJECTORY,
                HBaseConstant.COLUMN_GPS);

        String[] gpsSplits = result.substring(1, result.length() - 1).split(", ");
//        Stream.of(gpsSplits).map(x->new GPS(x)).collect(Collectors.toList())
//        List<GPS> tileList = new ArrayList<>();
//        for (String tile : tileSplits) {
//            tileList.add(new GPS(tile));
//        }
        return Stream.of(gpsSplits).map(GPS::new).collect(Collectors.toList());
    }

    /**
     * Acquire the trajectoryPoints of trajectoryID from HBase
     */
    public static List<Cell> getTrajectoryCells(String trajectoryID) {
        String result = hBaseUtil.getColumnFromHBase(
                HBaseConstant.TABLE_SH_TRAJECTORY,
                trajectoryID.getBytes(),
                HBaseConstant.COLUMN_FAMILY_TRAJECTORY,
                HBaseConstant.COLUMN_CELL);

        String[] tileSplits = result.substring(1, result.length() - 1).split(", ");

        List<Cell> cellList = new ArrayList<>();
        for (String tile : tileSplits) {
            cellList.add(new Cell(tile));
        }
        return cellList;
    }

    private static Map<String, List<Cell>> getTrajectoryCells(List<String> trajectoryID, String city) {
        List<byte[]> trajectoryBytes = trajectoryID.stream().map(String::getBytes).collect(Collectors.toList());
        Map<String, String> queryResult = hBaseUtil.getBatchColumnFromHBase(
                CommonUtil.getTrajectoryTable(city),
                trajectoryBytes,
                HBaseConstant.COLUMN_FAMILY_TRAJECTORY,
                HBaseConstant.COLUMN_CELL);

        Map<String, List<Cell>> result = new HashMap<>();
        for (Map.Entry<String, String> entry : queryResult.entrySet()) {
            String s = entry.getValue();
            result.put(entry.getKey(), Arrays.stream(s.substring(1, s.length() - 1).split(", ")).map(Cell::new).collect(Collectors.toList()));
        }

        return result;
    }

    private static Map<String, List<GPS>> getTrajectoryGPSs(List<String> trajectoryID, String city) {
        List<byte[]> trajectoryBytes = trajectoryID.stream().map(String::getBytes).collect(Collectors.toList());
        Map<String, String> queryResult = hBaseUtil.getBatchColumnFromHBase(
                CommonUtil.getTrajectoryTable(city),
                trajectoryBytes,
                HBaseConstant.COLUMN_FAMILY_TRAJECTORY,
                HBaseConstant.COLUMN_GPS);

        Map<String, List<GPS>> result = new HashMap<>();
        for (Map.Entry<String, String> entry : queryResult.entrySet()) {
            String s = entry.getValue();
            result.put(entry.getKey(), Arrays.stream(s.substring(1, s.length() - 1).split(", ")).map(GPS::new).collect(Collectors.toList()));
        }

        return result;
    }

    /**
     * Acquire allTrajectoryID passed the start point and the end point.
     *
     * @param start start point
     * @param end   end point
     */
    private static Set<String> getAllTrajectoryID(Cell start, Cell end, String city) {
        Map<String, String> startMaps = hBaseUtil.getColumnFamilyFromHBase(
                CommonUtil.getInvertedTable(city),
                start.toString().getBytes(),
                HBaseConstant.COLUMN_FAMILY_INDEX);
        Map<String, String> endMaps = hBaseUtil.getColumnFamilyFromHBase(
                CommonUtil.getInvertedTable(city),
                end.toString().getBytes(),
                HBaseConstant.COLUMN_FAMILY_INDEX);

        return getAllTrajectoryID(startMaps, endMaps);
    }

    public static Map<String, List<Cell>> getAllTrajectoryCells(Cell start, Cell end, String city) {
        Map<String, String> startMaps = hBaseUtil.getColumnFamilyFromHBase(
                CommonUtil.getInvertedTable(city),
                start.toString().getBytes(),
                HBaseConstant.COLUMN_FAMILY_INDEX);
        Map<String, String> endMaps = hBaseUtil.getColumnFamilyFromHBase(
                CommonUtil.getInvertedTable(city),
                end.toString().getBytes(),
                HBaseConstant.COLUMN_FAMILY_INDEX);

        Set<String> trajectoryID = getAllTrajectoryID(startMaps, endMaps);

        Map<String, List<Cell>> allTrajectories = getTrajectoryCells(new ArrayList<>(trajectoryID), city);
        for (String trajectoryId : trajectoryID) {

            int startIndex = Integer.parseInt(startMaps.get(trajectoryId));
            int endIndex = Integer.parseInt(endMaps.get(trajectoryId));

            //Only reserve the part from the start point to the end point.
            allTrajectories.put(trajectoryId, allTrajectories.get(trajectoryId).subList(startIndex, endIndex + 1));
        }
        return allTrajectories;
    }

    public static Map<String, List<GPS>> getAllTrajectoryGPSs(Cell start, Cell end, String city) {
        Set<String> trajectoryID = getAllTrajectoryID(start, end, city);
        Map<String, List<GPS>> allTrajectories = getTrajectoryGPSs(new ArrayList<>(trajectoryID), city);
        for (String trajectoryId : trajectoryID) {
            List<GPS> cutTrajectory = removeExtraGPS(allTrajectories.get(trajectoryId), start, end);
            if (cutTrajectory != null)
                allTrajectories.put(trajectoryId, cutTrajectory);
            else
                allTrajectories.remove(trajectoryId);
        }
        return allTrajectories;
    }

    public static List<GPS> removeExtraGPS(List<GPS> gpsTrajectory, Cell start, Cell end) {
        int startIndex = -1, endIndex = -1;
        for (int i = 0; i < gpsTrajectory.size(); i++) {
            if (TileSystem.GPSToTile(gpsTrajectory.get(i)).equals(start)) {
                startIndex = i;
            } else if (TileSystem.GPSToTile(gpsTrajectory.get(i)).equals(end)) {
                endIndex = i;
                break;
            }
        }
        try {
            if (startIndex != -1 && endIndex != -1)
                return gpsTrajectory.subList(startIndex, endIndex);
            else
                return null;
        } catch (Exception e) {
            return null;
        }
    }

    private static Set<String> getAllTrajectoryID(Map<String, String> startMaps, Map<String, String> endMaps) {
        //compute the intersection of two trajectory set,
        //the intersection denote all the trajectory that pass the start point and the end point.
        Set<String> trajectoryIntersection = startMaps.keySet();
        trajectoryIntersection.retainAll(endMaps.keySet());

        //remove the trajectory that is from the end point to the start point.
        Set<String> reverseTrajectory = new HashSet<>();
        for (String trajectoryId : trajectoryIntersection) {
            int startIndex = Integer.parseInt(startMaps.get(trajectoryId));
            int endIndex = Integer.parseInt(endMaps.get(trajectoryId));
            if (endIndex < startIndex)
                reverseTrajectory.add(trajectoryId);
        }
        trajectoryIntersection.removeAll(reverseTrajectory);

        return trajectoryIntersection;
    }

}
