package bean;

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
        this.timestamp = timestamp;
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
        return "[" + longitude + "," + latitude + "," + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamp) + "]";
    }

}
