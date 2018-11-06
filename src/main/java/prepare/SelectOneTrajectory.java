package prepare;

import bean.GPS;
import com.google.gson.GsonBuilder;
import hbase.TrajectoryUtil;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by cbdog94 on 17-3-17.
 */
public class SelectOneTrajectory {

    public static void main(String[] args) {

//        if (args.length != 1) {
//            System.out.println("please input trajectoryID");
//            return;
//        }
        List<GPS> trajectory = TrajectoryUtil.getTrajectoryGPSPoints("fe4186d55580490ea429c10d26d187d2");
        List<GPSOut> trajectoryOut = trajectory.stream().map(GPSOut::new).collect(Collectors.toList());
        System.out.println(trajectoryOut.size());
        String output = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ssZZZ").create().toJson(trajectoryOut.subList(200, 241));
        System.out.println(output);
//        FileUtils.write(FileUtils.getFile("/var/www/html/used/map_origin.json"),output,"UTF-8");
    }

    static class GPSOut {

        //        String point;
        double latitude;
        double longitude;
        long loc_time;
        String coord_type_input = "wgs84";
        String entity_name = "1";
//        String id="12345";

        public GPSOut(GPS gps) {
            loc_time = gps.getTimestamp().getTime() / 1000 + 3 * 365 * 24 * 3600;
//            point=String.format("POINT(%f %f)",gps.getLongitude(),gps.getLatitude());
//            double[] bdPoint=GeoUtil.wgs2bd(new double[]{gps.getLatitude(),gps.getLongitude()});
//            latitude = bdPoint[0];
//            longitude = bdPoint[1];
            latitude = gps.getLatitude();
            longitude = gps.getLongitude();
        }
    }

}
