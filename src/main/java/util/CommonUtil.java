package util;

import bean.Cell;
import bean.GPS;
import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import constant.HBaseConstant;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Common utility
 *
 * @author Bin Cheng
 */
public class CommonUtil {

    /**
     * Generate the UUID without '-'
     *
     * @return UUID
     */
    private static String getUUID() {
        String s = UUID.randomUUID().toString();
        return s.substring(0, 8) + s.substring(9, 13) + s.substring(14, 18) + s.substring(19, 23) + s.substring(24);
    }

    /**
     * @return unit:m
     */
    public static double distanceBetween(GPS GPS1, GPS GPS2) {
        return computeDistance(GPS1.getLatitude(), GPS1.getLongitude(), GPS2.getLatitude(), GPS2.getLongitude());
    }

    /**
     * @return unit:s
     */
    public static double timeBetween(GPS GPS1, GPS GPS2) {
        if (GPS1.getTimestamp() == null || GPS2.getTimestamp() == null)
            return -1;
        return Math.abs(GPS1.getTimestamp().getTime() - GPS2.getTimestamp().getTime()) / 1000;
    }

    /**
     * @return the distance of trajectory
     */
    public static double trajectoryDistance(List<GPS> trajectory) {
        if (trajectory.size() <= 1)
            return 0;

        double distance = 0.0;
        GPS lastGPS = trajectory.get(0);
        GPS currentGPS;
        for (int i = 1; i < trajectory.size(); i++) {
            currentGPS = trajectory.get(i);
            distance += distanceBetween(lastGPS, currentGPS);
            lastGPS = currentGPS;
        }
        return distance;
    }

    /**
     * Compute the distance between (lat1,lon1) and (lat2,lon2).
     * Copy from the method of Location.distanceBetween in Android.
     */
    private static double computeDistance(double lat1, double lon1,
                                          double lat2, double lon2) {
        // Based on http://www.ngs.noaa.gov/PUBS_LIB/inverse.pdf
        // using the "Inverse Formula" (section 4)

        int MAXITERS = 20;
        // Convert lat/long to radians
        lat1 *= Math.PI / 180.0;
        lat2 *= Math.PI / 180.0;
        lon1 *= Math.PI / 180.0;
        lon2 *= Math.PI / 180.0;

        double a = 6378137.0; // WGS84 major axis
        double b = 6356752.3142; // WGS84 semi-major axis
        double f = (a - b) / a;
        double aSqMinusBSqOverBSq = (a * a - b * b) / (b * b);

        double L = lon2 - lon1;
        double A = 0.0;
        double U1 = Math.atan((1.0 - f) * Math.tan(lat1));
        double U2 = Math.atan((1.0 - f) * Math.tan(lat2));

        double cosU1 = Math.cos(U1);
        double cosU2 = Math.cos(U2);
        double sinU1 = Math.sin(U1);
        double sinU2 = Math.sin(U2);
        double cosU1cosU2 = cosU1 * cosU2;
        double sinU1sinU2 = sinU1 * sinU2;

        double sigma = 0.0;
        double deltaSigma = 0.0;
        double cosSqAlpha = 0.0;
        double cos2SM = 0.0;
        double cosSigma = 0.0;
        double sinSigma = 0.0;
        double cosLambda = 0.0;
        double sinLambda = 0.0;

        double lambda = L; // initial guess
        for (int iter = 0; iter < MAXITERS; iter++) {
            double lambdaOrig = lambda;
            cosLambda = Math.cos(lambda);
            sinLambda = Math.sin(lambda);
            double t1 = cosU2 * sinLambda;
            double t2 = cosU1 * sinU2 - sinU1 * cosU2 * cosLambda;
            double sinSqSigma = t1 * t1 + t2 * t2; // (14)
            sinSigma = Math.sqrt(sinSqSigma);
            cosSigma = sinU1sinU2 + cosU1cosU2 * cosLambda; // (15)
            sigma = Math.atan2(sinSigma, cosSigma); // (16)
            double sinAlpha = (sinSigma == 0) ? 0.0 : cosU1cosU2 * sinLambda / sinSigma; // (17)
            cosSqAlpha = 1.0 - sinAlpha * sinAlpha;
            cos2SM = (cosSqAlpha == 0) ? 0.0 : cosSigma - 2.0 * sinU1sinU2 / cosSqAlpha; // (18)
            double uSquared = cosSqAlpha * aSqMinusBSqOverBSq; // defn
            A = 1 + (uSquared / 16384.0) * (4096.0 + uSquared * (-768 + uSquared * (320.0 - 175.0 * uSquared)));// (3)
            double B = (uSquared / 1024.0) * (256.0 + uSquared * (-128.0 + uSquared * (74.0 - 47.0 * uSquared)));// (4)
            double C = (f / 16.0) * cosSqAlpha * (4.0 + f * (4.0 - 3.0 * cosSqAlpha)); // (10)
            double cos2SMSq = cos2SM * cos2SM;
            deltaSigma = B * sinSigma * (cos2SM + (B / 4.0) * (cosSigma * (-1.0 + 2.0 * cos2SMSq) - (B / 6.0) * cos2SM * (-3.0 + 4.0 * sinSigma * sinSigma) * (-3.0 + 4.0 * cos2SMSq)));// (6)
            lambda = L + (1.0 - C) * f * sinAlpha * (sigma + C * sinSigma * (cos2SM + C * cosSigma * (-1.0 + 2.0 * cos2SM * cos2SM))); // (11)
            double delta = (lambda - lambdaOrig) / lambda;
            if (Math.abs(delta) < 1.0e-12) {
                break;
            }
        }

        return (b * A * (sigma - deltaSigma));
    }

    /**
     * Check if <b>timestamp</b> is the form of yyyy-MM-dd HH:mm:ss
     *
     * @param timestamp the timestamp which need to check
     * @return <b>true</b> the timestamp is valid, <b>false</b> the timestamp is invalid.
     */
    public static boolean isValidTimestamp(String timestamp) {
        return Pattern.compile("\\w{4}-\\w{2}-\\w{2} \\w{2}:\\w{2}:\\w{2}").matcher(timestamp).matches();
    }

    public static List<GPS> removeExtraGPS(List<GPS> gpsTrajectory, Cell start, Cell end) {
        int startIndex = 0;
        int endIndex = 0;
        for (int i = 0; i < gpsTrajectory.size(); i++) {
            if (TileSystem.GPSToTile(gpsTrajectory.get(i)).equals(start))
                startIndex = i;
            else if (TileSystem.GPSToTile(gpsTrajectory.get(i)).equals(end))
                endIndex = i;
        }
        try {
            return gpsTrajectory.subList(startIndex, endIndex);
        } catch (Exception e) {
            return null;
        }

    }

    /**
     * 计算分位数
     */
    public static double percentile(double[] data, double p) {
        int n = data.length;
        Arrays.sort(data);
        double px = p * (n - 1);
        int i = (int) java.lang.Math.floor(px);
        double g = px - i;
        if (g == 0) {
            return data[i];
        } else {
            return (1 - g) * data[i] + g * data[i + 1];
        }
    }

    private static final double DISTANCE_LIMIT = 65 * 1000;

    public static String getTrajectoryTable(String city) {
        switch (city) {
            case "SH":
                return HBaseConstant.TABLE_SH_TRAJECTORY;
            case "SZ":
                return HBaseConstant.TABLE_SZ_TRAJECTORY;
            case "CD":
                return HBaseConstant.TABLE_CD_TRAJECTORY;
        }
        return null;
    }

    public static String getInvertedTable(String city) {
        switch (city) {
            case "SH":
                return HBaseConstant.TABLE_SH_TRAJECTORY_INVERTED;
            case "SZ":
                return HBaseConstant.TABLE_SZ_TRAJECTORY_INVERTED;
            case "CD":
                return HBaseConstant.TABLE_CD_TRAJECTORY_INVERTED;
        }
        return null;
    }

    public static boolean isValidCity(String city) {
        return city.equals("SH") || city.equals("SZ") || city.equals("CD");
    }

    public static class CityValidator implements IParameterValidator {
        @Override
        public void validate(String name, String value)
                throws ParameterException {
            if (!CommonUtil.isValidCity(value)) {
                throw new ParameterException("Parameter " + name + " should be 'SH', 'SZ' or 'CD' (found " + value + ")");
            }
        }
    }

    public static class CellValidator implements IParameterValidator {
        @Override
        public void validate(String name, String value)
                throws ParameterException {
            if (!Pattern.matches("\\[\\d+,\\d+]", value)) {
                throw new ParameterException("Parameter " + name + " should be [****,****].");
            }
        }
    }

    /**
     * Map the GPS trajectory to Cell trajectory, and convert the form of data so that it can be inserted into HBase.
     */
    public static Put preProcessTrajectory(String taxiID, List<GPS> trajectory) {

        //ignore while the trajectory is too long.
        double distance = util.CommonUtil.trajectoryDistance(trajectory);
        if (distance > DISTANCE_LIMIT)
            return null;

        //generate grid cell trajectory.
        List<Cell> cells = GridUtil.gridGPSSequence(trajectory);

        //if the number of cells are less than 5, we suppose that this trajectory is noise data.
        if (cells.size() < 5)
            return null;

        Put put = new Put(Bytes.toBytes(util.CommonUtil.getUUID()));
        put.addColumn(HBaseConstant.COLUMN_FAMILY_INFO, HBaseConstant.COLUMN_ID, Bytes.toBytes(taxiID));
        put.addColumn(HBaseConstant.COLUMN_FAMILY_TRAJECTORY, HBaseConstant.COLUMN_CELL, Bytes.toBytes(cells.toString()));
        put.addColumn(HBaseConstant.COLUMN_FAMILY_TRAJECTORY, HBaseConstant.COLUMN_GPS, Bytes.toBytes(trajectory.toString()));
        put.addColumn(HBaseConstant.COLUMN_FAMILY_INFO, HBaseConstant.COLUMN_DISTANCE, Bytes.toBytes(distance));
        return put;

    }


    public static void saveObjToFile(Object o, String fileName) {
        try {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(fileName));
            oos.writeObject(o);
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static Object getObjFromFile(String fileName) {
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileName));
            return ois.readObject();
        } catch (ClassNotFoundException | IOException ignored) {

        }
        return null;
    }

    public static double[] trajectoryInfo(List<GPS> trajectory, String city) {
        double distance = 0, totalTime, waitTime = 0;
        GPS last = trajectory.get(0);
        for (GPS cur : trajectory) {
            double segDist = distanceBetween(cur, last);
            double segTime = timeBetween(cur, last);
            distance += segDist;
            //lower than 12Km/h
            if (segTime > 0 && segDist / segTime < 3.33) {
                waitTime += segTime;
            }
            last = cur;
        }
        totalTime = timeBetween(trajectory.get(0), trajectory.get(trajectory.size() - 1));
        double fare = 0;
        switch (city) {
            case "SH":
                if (distance <= 3000)
                    fare = 14;
                else if (distance <= 15000)
                    fare = 14 + 2.5 * (distance / 1000 - 3);
                else
                    fare = 14 + 30 + 3.6 * (distance / 1000 - 15);
                fare += Math.floor(waitTime / 60 / 4) * 2.5;
                break;
            case "SZ":
                if (distance <= 2000)
                    fare = 11;
                else if (distance <= 25000)
                    fare = 11 + 2.4 * (distance / 1000 - 2);
                else
                    fare = 11 + 55.2 + 3.12 * (distance / 1000 - 25);
                fare += Math.floor(waitTime / 60) * 0.8;
                break;
            case "CD":
                if (distance <= 2000)
                    fare = 8;
                else if (distance <= 10000)
                    fare = 8 + 1.9 * (distance / 1000 - 2);
                else
                    fare = 8 + 15.2 + 2.85 * (distance / 1000 - 10);
                fare += Math.floor(waitTime / 60 / 5) * 1.9;
                break;
        }
        return new double[]{distance, totalTime, fare};
    }

    public static Set<String> anomalyTrajectory(Map<String, List<GPS>> originTrajectory, String city, boolean debug) {
        Map<String, Double> fareMap = new HashMap<>();
        for (Map.Entry<String, List<GPS>> entry : originTrajectory.entrySet()) {
            fareMap.put(entry.getKey(), trajectoryInfo(entry.getValue(), city)[2]);
        }
        double threshold = percentile(fareMap.values().stream().mapToDouble(s -> s).toArray(), 0.95);
        if (debug) {
            System.out.println("Fare List: " + fareMap.values());
            System.out.println("Fare threshold: " + threshold);
        }
        return fareMap.entrySet().stream().filter(s -> s.getValue() > threshold).map(Map.Entry::getKey).collect(Collectors.toSet());
    }

    public static void printResult(int TP, int FP, int FN, int TN) {
        System.out.println("-----------");
        System.out.println("TP: " + TP);
        System.out.println("FP: " + FP);
        System.out.println("FN: " + FN);
        System.out.println("TN: " + TN);

        System.out.println("TPR: " + TP * 1.0 / (TP + FN));
        System.out.println("FPR: " + FP * 1.0 / (FP + TN));
        System.out.println("-----------");
    }

}
