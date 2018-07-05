package web;

import algorithm.STLDetection;
import algorithm.iBOATDetection;
import bean.Cell;
import bean.GPS;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import constant.CommonConstant;
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
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Offline Detect
 *
 * @author Bin Cheng
 */
public class OfflineDetect extends HttpServlet {

    private static Set<String> idSet = new HashSet<>();

    private ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat("offline-detection-pool-%d").build();
    private ExecutorService threadPool = new ThreadPoolExecutor(5, 5,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1024), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());


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

                threadPool.execute(() -> detect(id, startPoint, endPoint));

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

        Cell startCell = TileSystem.gpsToTile(startPoint);
        Cell endCell = TileSystem.gpsToTile(endPoint);

        // GPS trajectory.
        Map<String, List<GPS>> trajectoryGPS = TrajectoryUtil.getAllTrajectoryGPSs(startCell, endCell, CommonConstant.SHANGHAI);
        // Cell trajectory.
        Map<String, List<Cell>> trajectoryCell = Maps.filterKeys(TrajectoryUtil.getAllTrajectoryCells(startCell, endCell, CommonConstant.SHANGHAI), trajectoryGPS::containsKey);

        threadPool.execute(
                () -> {

                    List<TrajectoryInfo> iBOATTrajectoryInfo = trajectoryGPS.entrySet().parallelStream()
                            .map(entry -> {
                                        Map<String, List<Cell>> tmp = new HashMap<>(trajectoryCell);
                                        tmp.remove(entry.getKey());
                                        double score = iBOATDetection.iBOAT(entry.getValue(), new ArrayList<>(tmp.values()));
                                        return new TrajectoryInfo(entry.getKey(), score, score < 0.1, web.CommonUtil.singleCompress(entry.getValue()));

                                    }
                            ).collect(Collectors.toList());

                    String root = getServletContext().getRealPath("/");
                    File file = FileUtils.getFile(root + "offlineIBOAT_" + id + ".json");
                    String content = new Gson().toJson(iBOATTrajectoryInfo);
                    try {
                        FileUtils.write(file, content, false);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    WebSocketOffline.sendMessage(id, "iBOATDone");
                });

        // Trajectory info.
        Map<String, double[]> trajectoryInfoMap = Maps.transformValues(trajectoryGPS, s -> CommonUtil.trajectoryInfo(s, "SH"));

        // Generate training set.
        double[] distArray = trajectoryInfoMap.values().stream().mapToDouble(s -> s[0]).toArray();
        double[] timeArray = trajectoryInfoMap.values().stream().mapToDouble(s -> s[1]).toArray();

        double distance60 = CommonUtil.percentile(distArray, 0.6);
        double time60 = CommonUtil.percentile(timeArray, 0.6);

        Map<String, List<GPS>> trainTrajectory = Maps.filterKeys(trajectoryGPS, Maps.filterValues(trajectoryInfoMap, s -> s[0] < distance60 || s[1] < time60)::containsKey);

        // Detection.
        List<TrajectoryInfo> STLTrajectoryInfo = trajectoryGPS.entrySet().parallelStream()
                .map(entry -> {
                            Map<String, List<Cell>> tmp = new HashMap<>(trajectoryCell);
                            tmp.remove(entry.getKey());
                            double score = STLDetection.detect(new ArrayList<>(trainTrajectory.values()), entry.getValue());
                            return new TrajectoryInfo(entry.getKey(), score, score < 0.1, web.CommonUtil.singleCompress(entry.getValue()));

                        }
                ).collect(Collectors.toList());

        String root = getServletContext().getRealPath("/");
        File file = FileUtils.getFile(root + "offlineSTL_" + id + ".json");
        String content = new Gson().toJson(STLTrajectoryInfo);
        try {
            FileUtils.write(file, content, false);
        } catch (IOException e) {
            e.printStackTrace();
        }

        WebSocketOffline.sendMessage(id, "STLDone");

    }

    @Override
    public void destroy() {
        threadPool.shutdown();
        super.destroy();
    }
}