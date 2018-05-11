package constant;

/**
 * HBase constant
 *
 * @author Bin Cheng
 */
public class HBaseConstant {

    public static final String ZOOKEEPER_HOST = "192.168.0.51";
    public static final String ZOOKEEPER_PORT = "2181";

    public static final String TABLE_SH_TRAJECTORY = "sh_taxi_trajectory";
    public static final String TABLE_SZ_TRAJECTORY = "sz_taxi_trajectory";
    public static final String TABLE_CD_TRAJECTORY = "cd_taxi_trajectory";

    public static final byte[] COLUMN_FAMILY_INFO = "taxi_info".getBytes();
    public static final byte[] COLUMN_ID = "taxi_id".getBytes();
    public static final byte[] COLUMN_DISTANCE = "distance".getBytes();
    public static final byte[] COLUMN_FAMILY_TRAJECTORY = "taxi_trajectory".getBytes();
    public static final byte[] COLUMN_CELL = "taxi_cell".getBytes();
    public static final byte[] COLUMN_GPS = "taxi_gps".getBytes();


    public static final String TABLE_SH_TRAJECTORY_INVERTED = "sh_taxi_trajectory_inverted";
    public static final String TABLE_SZ_TRAJECTORY_INVERTED = "sz_taxi_trajectory_inverted";
    public static final String TABLE_CD_TRAJECTORY_INVERTED = "cd_taxi_trajectory_inverted";
    public static final byte[] COLUMN_FAMILY_INDEX = "trajectory_index".getBytes();

}
