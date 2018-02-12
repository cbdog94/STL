package hbase;

import bean.Cell;
import bean.GPS;
import constant.HBaseConstant;

import java.util.*;

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
    public static List<Cell> getTrajectoryCells(String trajectoryID) {
        String result = hBaseUtil.getColumnFromHBase(
                HBaseConstant.TABLE_TRAJECTORY,
                trajectoryID.getBytes(),
                HBaseConstant.COLUMN_FAMILY_TRAJECTORY,
                HBaseConstant.COLUMN_TRAJECTORY);

        String[] tileSplits = result.substring(1, result.length() - 1).split(", ");

        List<Cell> cellList = new ArrayList<>();
        for (String tile : tileSplits) {
            cellList.add(new Cell(tile));
        }
        return cellList;
    }

    /**
     * Acquire the trajectoryPoints of trajectoryID from HBase
     */
    public static List<GPS> getTrajectoryGPSPoints(String trajectoryID) {
        String result = hBaseUtil.getColumnFromHBase(
                HBaseConstant.TABLE_TRAJECTORY,
                trajectoryID.getBytes(),
                HBaseConstant.COLUMN_FAMILY_TRAJECTORY,
                HBaseConstant.COLUMN_GPS);

        String[] tileSplits = result.substring(1, result.length() - 1).split(", ");

        List<GPS> tileList = new ArrayList<>();
        for (String tile : tileSplits) {
            tileList.add(new GPS(tile));
        }
        return tileList;
    }

    /**
     * Acquire allTrajectoryPoints passed the start point and the end point.
     *
     * @param start start point
     * @param end   end point
     */
    public static Map<String, List<Cell>> getAllTrajectoryCells(Cell start, Cell end) {
        Map<String, String> startMaps = hBaseUtil.getColumnFamilyFromHBase(
                HBaseConstant.TABLE_TRAJECTORY_INVERTED,
                start.toString().getBytes(),
                HBaseConstant.COLUMN_FAMILY_INDEX);
        Map<String, String> endMaps = hBaseUtil.getColumnFamilyFromHBase(
                HBaseConstant.TABLE_TRAJECTORY_INVERTED,
                end.toString().getBytes(),
                HBaseConstant.COLUMN_FAMILY_INDEX);

        Set<String> trajectoryID = getAllTrajectoryID(startMaps, endMaps);

        Map<String, List<Cell>> allTrajectories = new HashMap<>();
        for (String trajectoryId : trajectoryID) {
            //The trajectory passed the start point and the end point,
            // but may not start at the start point or end at the end point.
            List<Cell> trajectory = getTrajectoryCells(trajectoryId);

            int startIndex = Integer.parseInt(startMaps.get(trajectoryId));
            int endIndex = Integer.parseInt(endMaps.get(trajectoryId));

            //Only reserve the part from the start point to the end point.
            allTrajectories.put(trajectoryId, new ArrayList<>(trajectory.subList(startIndex, endIndex + 1)));
        }
        return allTrajectories;

    }

    /**
     * Acquire allTrajectoryID passed the start point and the end point.
     *
     * @param start start point
     * @param end   end point
     */
    public static Set<String> getAllTrajectoryID(Cell start, Cell end) {
        Map<String, String> startMaps = hBaseUtil.getColumnFamilyFromHBase(
                HBaseConstant.TABLE_TRAJECTORY_INVERTED,
                start.toString().getBytes(),
                HBaseConstant.COLUMN_FAMILY_INDEX);
        Map<String, String> endMaps = hBaseUtil.getColumnFamilyFromHBase(
                HBaseConstant.TABLE_TRAJECTORY_INVERTED,
                end.toString().getBytes(),
                HBaseConstant.COLUMN_FAMILY_INDEX);

        return getAllTrajectoryID(startMaps, endMaps);
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

    public static List<List<Cell>> getAllTrajectoryCell(Set<String> trajectoryID) {
        List<List<Cell>> result = new ArrayList<>();
        for (String id : trajectoryID) {
            result.add(getTrajectoryCells(id));
        }
        return result;
    }

    public static List<List<GPS>> getAllTrajectoryGPS(Set<String> trajectoryID) {
        List<List<GPS>> result = new ArrayList<>();
        for (String id : trajectoryID) {
            result.add(getTrajectoryGPSPoints(id));
        }
        return result;
    }

}
