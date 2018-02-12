package prepare;

import com.google.gson.Gson;
import mapreduce.DailyTrajectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by cbdog94 on 2017/4/12.
 */
public class ExtractTurningFeature {
    public static void main(String[] args) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(new File("/Library/WebServer/Documents/used/30040.json")));
        String content = reader.readLine();
        reader.close();
//        System.out.println(content);
        DailyTrajectory.Trajectory trajectory = new Gson().fromJson(content, DailyTrajectory.Trajectory.class);
//        System.out.println(trajectory.taxiId);
        System.out.println(trajectory.points);

        List<PointInfo> points = new ArrayList<>();
        List<List<PointInfo>> trajectories = new ArrayList<>();
        boolean flag = false;
        for (DailyTrajectory.PointInfo point : trajectory.points) {
            if (point.status == 0) {
                points.add(new PointInfo(point));
                flag = true;
            } else if (point.status == 1 && flag) {
                trajectories.add(points);
                points = new ArrayList<>();
                flag = false;
            }
        }
        System.out.println(trajectories.size());
        for (List<PointInfo> t : trajectories) {
            extract(t);
        }
        System.out.println(new Gson().toJson(trajectories));
    }

    private static void extract(List<PointInfo> t) {
        PointInfo start = t.get(0);
        double max = 0.0;
        double min = 0.0;
        for (PointInfo point : t) {
            double differ = point.direct - start.direct;
            if (differ > 180)
                differ -= 360;
            else if (differ < -180)
                differ += 360;
//            double differ=Math.abs(point.direct-start.direct)>180?360-Math.abs(point.direct-start.direct):Math.abs(point.direct-start.direct);
            if (differ > 45) {
                if (differ > max) {
                    max = differ;
                } else {
                    point.turn = max;
                    max = 0;
                    start = point;
                    System.out.println(point.timestamp);
                }
            } else if (differ < -45) {
                if (differ < min) {
                    min = differ;
                } else {
                    point.turn = min;
                    min = 0;
                    start = point;
                    System.out.println(point.timestamp);
                }
            }
        }
    }


    public static class PointInfo extends DailyTrajectory.PointInfo {
        public double turn;

        PointInfo(DailyTrajectory.PointInfo pointInfo) {
            this.lat = pointInfo.lat;
            this.lng = pointInfo.lng;
            this.status = pointInfo.status;
            this.timestamp = pointInfo.timestamp;
            this.speed = pointInfo.speed;
            this.direct = pointInfo.direct;
        }
    }
}
