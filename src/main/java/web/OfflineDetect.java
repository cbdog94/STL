package web;

import algorithm.STLDetection;
import algorithm.iBOATDetection;
import bean.Cell;
import bean.GPS;
import com.google.gson.Gson;
import hbase.HBaseUtil;
import hbase.TrajectoryUtil;
import org.apache.commons.io.FileUtils;
import util.CommonUtil;
import util.TileSystem;
import web.bean.TrajectoryInfo;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 离线检测
 */
public class OfflineDetect extends HttpServlet {

    private static Set<String> idSet = new HashSet<>();


    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
        String id = req.getParameter("id");
        if (!idSet.contains(id)) {
            try {
                idSet.add(id);
                String[] starts = req.getParameter("start").split(",");
                double startLatitude = Double.parseDouble(starts[0]);
                double startLongitude = Double.parseDouble(starts[1]);
                GPS startPoint = new GPS(startLatitude, startLongitude, new Date());

                String[] ends = req.getParameter("end").split(",");
                double endLatitude = Double.parseDouble(ends[0]);
                double endLongitude = Double.parseDouble(ends[1]);
                GPS endPoint = new GPS(endLatitude, endLongitude, new Date());

                new Thread(() -> detect(id, startPoint, endPoint)).start();

                web.CommonUtil.response(req, resp, 200, "init");
            } catch (Exception e) {
                e.printStackTrace();
                web.CommonUtil.response(req, resp, 500, "Please input correct start and end points!");
            }
        }
    }

    /**
     * 对经过起止点的所有轨迹进行检测，轨迹来源与数据库
     *
     * @param startPoint 起始点GPS坐标
     * @param endPoint   终止点GPS坐标
     */
    private void detect(String id, GPS startPoint, GPS endPoint) {

        Cell startCell = TileSystem.GPSToTile(startPoint);
        Cell endCell = TileSystem.GPSToTile(endPoint);

        System.out.println(startCell + " " + endCell);

        //First step，得到经过起止点所有轨迹
        Map<String, List<Cell>> allTrajectories = TrajectoryUtil.getAllTrajectoryCells(startCell, endCell, "SH");

        List<TrajectoryInfo> iBOATTrajectoryInfo = new ArrayList<>();
        List<TrajectoryInfo> iBOATNormalTrajectoryInfo = new ArrayList<>();

        Map<String, List<GPS>> normalTrajectory = new HashMap<>();
        Map<String, List<GPS>> anomalyTrajectory = new HashMap<>();

        //得到所有轨迹的ID集合，然后对每条轨迹进行检测
        for (String trajectoryID : allTrajectories.keySet()) {

            //Second,得到待测轨迹的GPS坐标序列
            List<GPS> testTrajectory = TrajectoryUtil.getTrajectoryGPSPoints(trajectoryID);
            testTrajectory = CommonUtil.removeExtraGPS(testTrajectory, startCell, endCell);
            if (testTrajectory == null || testTrajectory.size() == 0) {
                continue;
            }
            Map<String, List<Cell>> tmpTrajectories = new HashMap<>(allTrajectories);
            tmpTrajectories.remove(trajectoryID);

            //Third,进行检测
            double score = iBOATDetection.iBOAT(testTrajectory, new ArrayList<>(tmpTrajectories.values()), 0.04);

            TrajectoryInfo trajectoryInfo = new TrajectoryInfo();
            trajectoryInfo.taxiId = trajectoryID;
            trajectoryInfo.score = score;
            trajectoryInfo.normal = score < 0.1;
            trajectoryInfo.trajectory = compress(testTrajectory);

            iBOATTrajectoryInfo.add(trajectoryInfo);

            if (trajectoryInfo.normal) {
                normalTrajectory.put(trajectoryID, testTrajectory);
                iBOATNormalTrajectoryInfo.add(trajectoryInfo);
            } else {
                anomalyTrajectory.put(trajectoryID, testTrajectory);
            }

        }

        new Thread(() -> {
            String root = getServletContext().getRealPath("/");
            File file = FileUtils.getFile(root + "offlineIBOAT_" + id + ".json");
            String content = new Gson().toJson(iBOATTrajectoryInfo);
            try {
                FileUtils.write(file, content, "UTF-8", false);
            } catch (IOException e) {
                e.printStackTrace();
            }
            WebSocketOffline.sendMessage(id, "iBOATDone");
        }).start();

        List<TrajectoryInfo> DTMAnomlyTrajectoryInfo = new ArrayList<>();
        for (String trajectoryID : anomalyTrajectory.keySet()) {
            double score = STLDetection.detect(new ArrayList<>(normalTrajectory.values()), anomalyTrajectory.get(trajectoryID));
            TrajectoryInfo trajectoryInfo = new TrajectoryInfo();
            trajectoryInfo.taxiId = trajectoryID;
            trajectoryInfo.score = score;
            trajectoryInfo.normal = score < 0.1;
            trajectoryInfo.trajectory = compress(anomalyTrajectory.get(trajectoryID));
            if (trajectoryInfo.normal) {
                iBOATNormalTrajectoryInfo.add(trajectoryInfo);
            } else {
                DTMAnomlyTrajectoryInfo.add(trajectoryInfo);
            }

        }
        DTMAnomlyTrajectoryInfo.addAll(iBOATNormalTrajectoryInfo);

        String root = getServletContext().getRealPath("/");
        File file = FileUtils.getFile(root + "offlineDTM_" + id + ".json");
        String content = new Gson().toJson(DTMAnomlyTrajectoryInfo);
        try {
            FileUtils.write(file, content, "UTF-8", false);
        } catch (IOException e) {
            e.printStackTrace();
        }

        WebSocketOffline.sendMessage(id, "DTMDone");

    }

    private List<Object[]> compress(List<GPS> trajectoriesGPS) {

        List<Object[]> item = new ArrayList<>();
        for (GPS gps : trajectoriesGPS) {
            item.add(new Object[]{gps.getLatitude(), gps.getLongitude(), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(gps.getTimestamp())});
        }

        return item;
    }

    @Override
    public void destroy() {
        System.out.println("destroy");
        HBaseUtil.close();
        super.destroy();
    }
}