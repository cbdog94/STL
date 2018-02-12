package constant;

/**
 * HBase constant
 *
 * @author Bin Cheng
 */
public class HBaseConstant {

    public static final String TABLE_TRAJECTORY = "taxi_trajectory";
    public static final byte[] COLUMN_FAMILY_INFO = "taxi_info".getBytes();
    public static final byte[] COLUMN_ID = "taxi_id".getBytes();
    public static final byte[] COLUMN_DISTANCE = "distance".getBytes();
    public static final byte[] COLUMN_FAMILY_TRAJECTORY = "taxi_trajectory".getBytes();
    public static final byte[] COLUMN_TRAJECTORY = "taxi_trajectory".getBytes();
    public static final byte[] COLUMN_GPS = "taxi_gps".getBytes();


    public static final String TABLE_TRAJECTORY_INVERTED = "taxi_trajectory_inverted";
    public static final byte[] COLUMN_FAMILY_INDEX = "trajectory_index".getBytes();

}
