package bean;

import com.graphhopper.util.GPXEntry;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * GPS point
 *
 * @author Bin Cheng
 */
public class GPS implements Serializable {

    private double latitude;
    private double longitude;
    private Date timestamp;

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public GPS(double latitude, double longitude, Date timestamp) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = timestamp == null ? null : new Date(timestamp.getTime());
    }

    public GPS(GPS gps) {
        this.latitude = gps.latitude;
        this.longitude = gps.longitude;
        this.timestamp = gps.timestamp == null ? null : new Date(gps.timestamp.getTime());
    }

    /***
     * @param GPS eg:[121.534707,31.259012,2014-01-01 22:22:22]
     */
    public GPS(String GPS) {
        try {
            Matcher matcher = Pattern.compile("\\[([\\w|.]+),([\\w|.]+),(\\w{4}-\\w{2}-\\w{2} \\w{2}:\\w{2}:\\w{2})]").matcher(GPS);
            if (!matcher.lookingAt())
                return;
            this.longitude = Double.parseDouble(matcher.group(1));
            this.latitude = Double.parseDouble(matcher.group(2));
            this.timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(matcher.group(3));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        if (timestamp == null)
            return "[" + longitude + "," + latitude + "]";
        return "[" + longitude + "," + latitude + "," + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamp) + "]";
    }

    public GPXEntry convertToGPX(String city) {
        if (city.equals("CD")) {
            double[] wgsGPS = gcj2wgs(latitude, longitude);
            return new GPXEntry(wgsGPS[0], wgsGPS[1], timestamp.getTime());

        }
        return new GPXEntry(latitude, longitude, timestamp.getTime());
    }

    private double[] gcj2wgs(double lat, double lng) {
        double a = 6378245.0, ee = 0.00669342162296594323;
        double dLat = transformLat(lng - 105.0, lat - 35.0);
        double dLng = transformLon(lng - 105.0, lat - 35.0);
        double radLat = lat / 180.0 * Math.PI;
        double magic = Math.sin(radLat);
        magic = 1 - ee * magic * magic;
        double sqrtMagic = Math.sqrt(magic);
        dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * Math.PI);
        dLng = (dLng * 180.0) / (a / sqrtMagic * Math.cos(radLat) * Math.PI);
        double mgLat = lat + dLat;
        double mgLng = lng + dLng;
        return new double[]{lat * 2 - mgLat, lng * 2 - mgLng};
    }

    private double transformLat(double lat, double lon) {
        double ret = -100.0 + 2.0 * lat + 3.0 * lon + 0.2 * lon * lon + 0.1 * lat * lon + 0.2 * Math.sqrt(Math.abs(lat));
        ret += (20.0 * Math.sin(6.0 * lat * Math.PI) + 20.0 * Math.sin(2.0 * lat * Math.PI)) * 2.0 / 3.0;
        ret += (20.0 * Math.sin(lon * Math.PI) + 40.0 * Math.sin(lon / 3.0 * Math.PI)) * 2.0 / 3.0;
        ret += (160.0 * Math.sin(lon / 12.0 * Math.PI) + 320 * Math.sin(lon * Math.PI / 30.0)) * 2.0 / 3.0;
        return ret;
    }

    private double transformLon(double lat, double lon) {
        double ret = 300.0 + lat + 2.0 * lon + 0.1 * lat * lat + 0.1 * lat * lon + 0.1 * Math.sqrt(Math.abs(lat));
        ret += (20.0 * Math.sin(6.0 * lat * Math.PI) + 20.0 * Math.sin(2.0 * lat * Math.PI)) * 2.0 / 3.0;
        ret += (20.0 * Math.sin(lat * Math.PI) + 40.0 * Math.sin(lat / 3.0 * Math.PI)) * 2.0 / 3.0;
        ret += (150.0 * Math.sin(lat / 12.0 * Math.PI) + 300.0 * Math.sin(lat / 30.0 * Math.PI)) * 2.0 / 3.0;
        return ret;
    }

}
