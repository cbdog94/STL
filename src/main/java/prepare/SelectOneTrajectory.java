package prepare;

import bean.GPS;
import com.google.gson.Gson;
import hbase.TrajectoryUtil;

import java.io.IOException;
import java.util.List;

/**
 * Created by cbdog94 on 17-3-17.
 */
public class SelectOneTrajectory {

    public static void main(String[] args) throws IOException {

//        if (args.length != 1) {
//            System.out.println("please input trajectoryID");
//            return;
//        }
        List<GPS> trajectory = TrajectoryUtil.getTrajectoryGPSPoints("fe4186d55580490ea429c10d26d187d2");
        System.out.println(new Gson().toJson(trajectory));
    }

}