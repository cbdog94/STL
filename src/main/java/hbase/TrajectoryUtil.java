package hbase;

import bean.Cell;
import bean.GPS;
import com.google.common.collect.Maps;
import constant.HBaseConstant;
import util.CommonUtil;
import util.TileSystem;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.stream.Collectors;

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
        String result;
        try {
            result = hBaseUtil.getColumnFromHBase(
                    HBaseConstant.TABLE_SH_TRAJECTORY,
                    trajectoryID.getBytes("UTF-8"),
                    HBaseConstant.COLUMN_FAMILY_TRAJECTORY.getBytes("UTF-8"),
                    HBaseConstant.COLUMN_GPS.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }

        String[] tileSplits = result.substring(1, result.length() - 1).split(", ");

        List<GPS> tileList = new ArrayList<>();
        for (String tile : tileSplits) {
            tileList.add(new GPS(tile));
        }
        return tileList;
    }

    /**
     * Acquire the trajectoryPoints of trajectoryID from HBase
     */
    public static List<Cell> getTrajectoryCells(String trajectoryID) {
        String result;
        try {
            result = hBaseUtil.getColumnFromHBase(
                    HBaseConstant.TABLE_SH_TRAJECTORY,
                    trajectoryID.getBytes("UTF-8"),
                    HBaseConstant.COLUMN_FAMILY_TRAJECTORY.getBytes("UTF-8"),
                    HBaseConstant.COLUMN_CELL.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }

        String[] tileSplits = result.substring(1, result.length() - 1).split(", ");

        List<Cell> cellList = new ArrayList<>();
        for (String tile : tileSplits) {
            cellList.add(new Cell(tile));
        }
        return cellList;
    }

    private static Map<String, List<Cell>> getTrajectoryCells(List<String> trajectoryID, String city) {
        List<byte[]> trajectoryBytes = trajectoryID.stream().map(String::getBytes).collect(Collectors.toList());
        Map<String, String> queryResult;
        try {
            queryResult = hBaseUtil.getBatchColumnFromHBase(
                    CommonUtil.getTrajectoryTable(city),
                    trajectoryBytes,
                    HBaseConstant.COLUMN_FAMILY_TRAJECTORY.getBytes("UTF-8"),
                    HBaseConstant.COLUMN_CELL.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }

        return new HashMap<>(Maps.transformValues(queryResult, v -> Arrays.stream(v.substring(1, v.length() - 1).split(", ")).map(Cell::new).collect(Collectors.toList())));
    }

    private static Map<String, List<GPS>> getTrajectoryGPSs(List<String> trajectoryID, String city) {
        List<byte[]> trajectoryBytes = trajectoryID.stream().map(String::getBytes).collect(Collectors.toList());
        Map<String, String> queryResult;
        try {
            queryResult = hBaseUtil.getBatchColumnFromHBase(
                    CommonUtil.getTrajectoryTable(city),
                    trajectoryBytes,
                    HBaseConstant.COLUMN_FAMILY_TRAJECTORY.getBytes("UTF-8"),
                    HBaseConstant.COLUMN_GPS.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }

        return new HashMap<>(Maps.transformValues(queryResult, v -> Arrays.stream(v.substring(1, v.length() - 1).split(", ")).map(GPS::new).collect(Collectors.toList())));
    }

    /**
     * Acquire allTrajectoryID passed the start point and the end point.
     *
     * @param start start point
     * @param end   end point
     */
    public static Set<String> getAllTrajectoryID(Cell start, Cell end, String city) {
        try {
            Map<String, String> startMaps = hBaseUtil.getColumnFamilyFromHBase(
                    CommonUtil.getInvertedTable(city),
                    start.toString().getBytes("UTF-8"),
                    HBaseConstant.COLUMN_FAMILY_INDEX.getBytes("UTF-8"));
            Map<String, String> endMaps = hBaseUtil.getColumnFamilyFromHBase(
                    CommonUtil.getInvertedTable(city),
                    end.toString().getBytes("UTF-8"),
                    HBaseConstant.COLUMN_FAMILY_INDEX.getBytes("UTF-8"));

            return getAllTrajectoryID(startMaps, endMaps);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Map<String, List<Cell>> getAllTrajectoryCells(Cell start, Cell end, String city) {
        Map<String, String> startMaps, endMaps;
        try {
            startMaps = hBaseUtil.getColumnFamilyFromHBase(
                    CommonUtil.getInvertedTable(city),
                    start.toString().getBytes("UTF-8"),
                    HBaseConstant.COLUMN_FAMILY_INDEX.getBytes("UTF-8"));
            endMaps = hBaseUtil.getColumnFamilyFromHBase(
                    CommonUtil.getInvertedTable(city),
                    end.toString().getBytes("UTF-8"),
                    HBaseConstant.COLUMN_FAMILY_INDEX.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }

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
            if (cutTrajectory != null) {
                allTrajectories.put(trajectoryId, cutTrajectory);
            } else {
                allTrajectories.remove(trajectoryId);
            }
        }
        return allTrajectories;
    }

    public static List<GPS> removeExtraGPS(List<GPS> gpsTrajectory, Cell start, Cell end) {
        int startIndex = 0, endIndex = 0;
        boolean startFound = false, endFound = false;
        Cell cur;
        for (int i = 0; i < gpsTrajectory.size(); i++) {
            cur = TileSystem.gpsToTile(gpsTrajectory.get(i));
            if (TileSystem.equal(cur, start)) {
                startIndex = i;
                startFound = true;
            } else if (TileSystem.equal(cur, end)) {
                endIndex = i;
                endFound = true;
            }
        }
        try {
            if (startFound && endFound) {
                return gpsTrajectory.subList(startIndex, endIndex);
            } else {
                return null;
            }
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
            if (endIndex < startIndex) {
                reverseTrajectory.add(trajectoryId);
            }
        }
        trajectoryIntersection.removeAll(reverseTrajectory);

        return trajectoryIntersection;
    }

}
