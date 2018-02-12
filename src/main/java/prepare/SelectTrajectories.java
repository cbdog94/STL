package prepare;

import bean.Cell;
import bean.GPS;
import com.google.gson.Gson;
import hbase.TrajectoryUtil;
import util.CommonUtil;
import util.TileSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by cbdog94 on 17-3-17.
 */
public class SelectTrajectories {

    public static void main(String[] args) throws IOException {
        Cell startPoint, endPoint;

        if (args.length < 2) {
//         startPoint = new Cell("[219505,107119]");//延安路华山路(静安寺)
            //Level 18
//            startPoint = new Cell("[219552,107108]");//陆家嘴
//            endPoint = new Cell("[219747,107149]");//浦东机场

            //Level 17
            startPoint = new Cell("[109776,53554]");//陆家嘴
            endPoint = new Cell("[109873,53574]");//浦东机场

        } else {
            startPoint = new Cell(args[0]);
            endPoint = new Cell(args[1]);
        }

        printAllTrajectoryPoints(startPoint, endPoint);

    }

    private static void printAllTrajectoryPoints(Cell start, Cell end) {

        Set<String> trajectoryID = TrajectoryUtil.getAllTrajectoryID(start, end);

        List<Trajectory> trajectoryList=new ArrayList<>();

        for (String trajectoryId : trajectoryID) {
            List<GPS> gpsTrajectory = TrajectoryUtil.getTrajectoryGPSPoints(trajectoryId);
            gpsTrajectory = CommonUtil.removeExtraGPS(gpsTrajectory, start, end);
            if (gpsTrajectory == null)
                continue;
            trajectoryList.add(new Trajectory(trajectoryId,new Gson().toJson(gpsTrajectory)));
        }

        System.out.println(new Gson().toJson(trajectoryList));
    }

    static class Trajectory{
        String id;
        String coords;

        public Trajectory(String id, String coords) {
            this.id = id;
            this.coords = coords;
        }
    }



}
