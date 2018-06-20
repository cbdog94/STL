package util;

import bean.Cell;
import bean.GPS;
import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Maps;
import constant.CommonConstant;
import constant.HBaseConstant;

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

    private static Pattern timestampPattern = Pattern.compile("\\w{4}-\\w{2}-\\w{2} \\w{2}:\\w{2}:\\w{2}");

    private static final double CHENGDU_STAGE_1 = 2000;
    private static final double CHENGDU_STAGE_2 = 10000;
    private static final double SHANGHAI_STAGE_1 = 3000;
    private static final double SHANGHAI_STAGE_2 = 15000;
    private static final double SHENZHEN_STAGE_1 = 2000;
    private static final double SHENZHEN_STAGE_2 = 25000;

    /**
     * Generate the UUID without '-'
     *
     * @return UUID
     */
    public static String getUUID() {
        String s = UUID.randomUUID().toString();
        return s.substring(0, 8) + s.substring(9, 13) + s.substring(14, 18) + s.substring(19, 23) + s.substring(24);
    }

    /**
     * @return unit:m
     */
    public static double distanceBetween(GPS gps1, GPS gps2) {
        return computeDistance(gps1.getLatitude(), gps1.getLongitude(), gps2.getLatitude(), gps2.getLongitude());
    }

    /**
     * @return unit:s
     */
    public static double timeBetween(GPS gps1, GPS gps2) {
        if (gps1.getTimestamp() == null || gps2.getTimestamp() == null) {
            return -1;
        }
        return Math.abs(gps1.getTimestamp().getTime() - gps2.getTimestamp().getTime()) / 1000.0;
    }

    /**
     * @return the distance of trajectory
     */
    public static double trajectoryDistance(List<GPS> trajectory) {
        if (trajectory.size() <= 1) {
            return 0;
        }

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
     * Based on http://www.ngs.noaa.gov/PUBS_LIB/inverse.pdf
     * using the "Inverse Formula" (section 4)
     */
    private static double computeDistance(double lat1, double lon1,
                                          double lat2, double lon2) {

        int maxIters = 20;
        // Convert lat/long to radians
        lat1 *= Math.PI / 180.0;
        lat2 *= Math.PI / 180.0;
        lon1 *= Math.PI / 180.0;
        lon2 *= Math.PI / 180.0;

        // WGS84 major axis
        double a = 6378137.0;
        // WGS84 semi-major axis
        double b = 6356752.3142;
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

        // initial guess
        double lambda = L;
        for (int iter = 0; iter < maxIters; iter++) {
            double lambdaOrig = lambda;
            cosLambda = Math.cos(lambda);
            sinLambda = Math.sin(lambda);
            double t1 = cosU2 * sinLambda;
            double t2 = cosU1 * sinU2 - sinU1 * cosU2 * cosLambda;
            // (14)
            double sinSqSigma = t1 * t1 + t2 * t2;
            sinSigma = Math.sqrt(sinSqSigma);
            // (15)
            cosSigma = sinU1sinU2 + cosU1cosU2 * cosLambda;
            // (16)
            sigma = Math.atan2(sinSigma, cosSigma);
            // (17)
            double sinAlpha = (sinSigma == 0) ? 0.0 : cosU1cosU2 * sinLambda / sinSigma;
            cosSqAlpha = 1.0 - sinAlpha * sinAlpha;
            // (18)
            cos2SM = (cosSqAlpha == 0) ? 0.0 : cosSigma - 2.0 * sinU1sinU2 / cosSqAlpha;
            // defn
            double uSquared = cosSqAlpha * aSqMinusBSqOverBSq;
            // (3)
            A = 1 + (uSquared / 16384.0) * (4096.0 + uSquared * (-768 + uSquared * (320.0 - 175.0 * uSquared)));
            // (4)
            double B = (uSquared / 1024.0) * (256.0 + uSquared * (-128.0 + uSquared * (74.0 - 47.0 * uSquared)));
            // (10)
            double C = (f / 16.0) * cosSqAlpha * (4.0 + f * (4.0 - 3.0 * cosSqAlpha));
            double cos2SMSq = cos2SM * cos2SM;
            // (6)
            deltaSigma = B * sinSigma * (cos2SM + (B / 4.0) * (cosSigma * (-1.0 + 2.0 * cos2SMSq) - (B / 6.0) * cos2SM * (-3.0 + 4.0 * sinSigma * sinSigma) * (-3.0 + 4.0 * cos2SMSq)));
            // (11)
            lambda = L + (1.0 - C) * f * sinAlpha * (sigma + C * sinSigma * (cos2SM + C * cosSigma * (-1.0 + 2.0 * cos2SM * cos2SM)));
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
        return timestampPattern.matcher(timestamp).matches();
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

    public static String getTrajectoryTable(String city) {
        switch (city) {
            case CommonConstant.SHANGHAI:
                return HBaseConstant.TABLE_SH_TRAJECTORY;
            case CommonConstant.SHENZHEN:
                return HBaseConstant.TABLE_SZ_TRAJECTORY;
            case CommonConstant.CHENGDU:
                return HBaseConstant.TABLE_CD_TRAJECTORY;
            default:
                return null;
        }
    }

    public static String getInvertedTable(String city) {
        switch (city) {
            case CommonConstant.SHANGHAI:
                return HBaseConstant.TABLE_SH_TRAJECTORY_INVERTED;
            case CommonConstant.SHENZHEN:
                return HBaseConstant.TABLE_SZ_TRAJECTORY_INVERTED;
            case CommonConstant.CHENGDU:
                return HBaseConstant.TABLE_CD_TRAJECTORY_INVERTED;
            default:
                return null;
        }
    }

    public static class CityValidator implements IParameterValidator {
        @Override
        public void validate(String name, String value)
                throws ParameterException {
            boolean validCity = CommonConstant.SHANGHAI.equals(value) ||
                    CommonConstant.SHENZHEN.equals(value) ||
                    CommonConstant.CHENGDU.equals(value);
            if (!validCity) {
                throw new ParameterException("Parameter " + name + " should be 'SH', 'SZ' or 'CD' (found " + value + ")");
            }
        }
    }

    public static class CellValidator implements IParameterValidator {
        private String cellRegex = "\\[\\d+,\\d+]";

        @Override
        public void validate(String name, String value)
                throws ParameterException {
            if (!Pattern.matches(cellRegex, value)) {
                throw new ParameterException("Parameter " + name + " should be [****,****].");
            }
        }
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
            Object obj = ois.readObject();
            ois.close();
            return obj;
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
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
            case CommonConstant.SHANGHAI:
                if (distance <= SHANGHAI_STAGE_1) {
                    fare = 14;
                } else if (distance <= SHANGHAI_STAGE_2) {
                    fare = 14 + 2.5 * (distance / 1000 - 3);
                } else {
                    fare = 14 + 30 + 3.6 * (distance / 1000 - 15);
                }
                fare += Math.floor(waitTime / 60 / 4) * 2.5;
                break;
            case CommonConstant.SHENZHEN:
                if (distance <= SHENZHEN_STAGE_1) {
                    fare = 11;
                } else if (distance <= SHENZHEN_STAGE_2) {
                    fare = 11 + 2.4 * (distance / 1000 - 2);
                } else {
                    fare = 11 + 55.2 + 3.12 * (distance / 1000 - 25);
                }
                fare += Math.floor(waitTime / 60) * 0.8;
                break;
            case CommonConstant.CHENGDU:
                if (distance <= CHENGDU_STAGE_1) {
                    fare = 8;
                } else if (distance <= CHENGDU_STAGE_2) {
                    fare = 8 + 1.9 * (distance / 1000 - 2);
                } else {
                    fare = 8 + 15.2 + 2.85 * (distance / 1000 - 10);
                }
                fare += Math.floor(waitTime / 60 / 5) * 1.9;
                break;
            default:
        }
        return new double[]{distance, totalTime, fare};
    }

    public static Set<String> anomalyTrajectory(Map<String, List<GPS>> originTrajectory, String city, boolean debug) {
        Map<String, Double> fareMap = Maps.transformValues(originTrajectory, v -> trajectoryInfo(v, city)[2]);
        double threshold = percentile(fareMap.values().stream().mapToDouble(s -> s).toArray(), 0.95);
        if (debug) {
            System.out.println("Fare List: " + fareMap.values());
            System.out.println("Fare threshold: " + threshold);
        }
        return fareMap.entrySet().stream().filter(s -> s.getValue() > threshold).map(Map.Entry::getKey).collect(Collectors.toSet());
    }

    public static void printResult(int tp, int fp, int fn, int tn) {
        System.out.println("-----------");
        System.out.println("TP: " + tp);
        System.out.println("FP: " + fp);
        System.out.println("FN: " + fn);
        System.out.println("TN: " + tn);

        System.out.println("TPR: " + tp * 1.0 / (tp + fn));
        System.out.println("FPR: " + fp * 1.0 / (fp + tn));
        System.out.println("-----------");
    }

}
