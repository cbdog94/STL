package web;

import algorithm.iBOATDetection;
import bean.Cell;
import bean.GPS;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import hbase.TrajectoryUtil;
import org.apache.commons.io.FileUtils;
import util.TileSystem;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Online Detect.
 *
 * @author Bin Cheng
 */
public class OnlineDetect extends HttpServlet {

    private static Set<String> idSet = new HashSet<>();
    private static Map<String, GPS> starts = new HashMap<>();
    public static Map<String, GPS> ends = new HashMap<>();

    public static Map<String, List<Cell>> anomalyCells = new HashMap<>();
    public static Map<String, List<List<Cell>>> supportTrajectories = new HashMap<>();
    public static Map<String, List<Cell>> adaptiveWindow = new HashMap<>();
    public static Map<String, List<List<Cell>>> allTrajectories = new HashMap<>();
    public static Map<String, Double> score = new HashMap<>();
    public static Map<String, GPS> lastGPS = new HashMap<>();

    private static final int FINISHED = 3;

    private ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat("online-detection-pool-%d").build();
    private ExecutorService threadPool = new ThreadPoolExecutor(5, 5,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1024), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());


    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String id = req.getParameter("id");
        if (!idSet.contains(id)) {
            try {

                String[] starts = req.getParameter("start").split(",");
                double startLatitude = Double.parseDouble(starts[0]);
                double startLongitude = Double.parseDouble(starts[1]);
                GPS startPoint = new GPS(startLatitude, startLongitude, new Date());

                String[] ends = req.getParameter("end").split(",");
                double endLatitude = Double.parseDouble(ends[0]);
                double endLongitude = Double.parseDouble(ends[1]);
                GPS endPoint = new GPS(endLatitude, endLongitude, new Date());

                threadPool.execute(() -> initDetect(id, startPoint, endPoint));

                web.CommonUtil.response(req, resp, 200, "init");
            } catch (Exception e) {
                e.printStackTrace();
                web.CommonUtil.response(req, resp, 500, "Please input correct start and end points!");
            }
        } else {
            GPS point;
            try {
                String[] points = req.getParameter("point").split(",");
                double latitude = Double.parseDouble(points[0]);
                double longitude = Double.parseDouble(points[1]);
                point = new GPS(latitude, longitude, new Date());
            } catch (Exception e) {
                e.printStackTrace();
                web.CommonUtil.response(req, resp, 500, "Please input correct point!");
                return;
            }

            try {
                iBOATDetection.DetectResult detectResult = iBOATDetection.iBOATOnline(id, point);
                if (detectResult.code == FINISHED) {
                    finishDetect(id);
                }
                web.CommonUtil.response(req, resp, 200, new Gson().toJson(detectResult));
            } catch (Exception e) {
                e.printStackTrace();
                web.CommonUtil.response(req, resp, 500, "Detection error!");
            }

        }
    }


    private void initDetect(String id, GPS startPoint, GPS endPoint) {
        idSet.add(id);
        starts.put(id, startPoint);
        ends.put(id, endPoint);

        Cell startCell = TileSystem.gpsToTile(startPoint);
        Cell endCell = TileSystem.gpsToTile(endPoint);

        Map<String, List<GPS>> allTrajectoryGPSs = TrajectoryUtil.getAllTrajectoryGPSs(startCell, endCell, "SH");

        //write file
        threadPool.execute(() -> {
            String root = getServletContext().getRealPath("/");
            File file = FileUtils.getFile(root + "detect_" + id + ".json");

            String content = new Gson().toJson(web.CommonUtil.multiCompress(new ArrayList<>(allTrajectoryGPSs.values())));
            try {
                FileUtils.write(file, content, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
            WebSocketOnline.sendMessage(id, "historyDone");
        });

        List<List<Cell>> allTrajectoriesList = new ArrayList<>(TrajectoryUtil.getAllTrajectoryCells(startCell, endCell, "SH").values());

        anomalyCells.put(id, new ArrayList<>());
        adaptiveWindow.put(id, new ArrayList<>());

        allTrajectories.put(id, allTrajectoriesList);
        supportTrajectories.put(id, new ArrayList<>(allTrajectoriesList));

        score.put(id, 0.0);
        lastGPS.put(id, startPoint);

        WebSocketOnline.sendMessage(id, "initDone");

    }

    private void finishDetect(String id) {
        idSet.remove(id);
        starts.remove(id);
        ends.remove(id);
        anomalyCells.remove(id);
        supportTrajectories.remove(id);
        adaptiveWindow.remove(id);
        allTrajectories.remove(id);
        score.remove(id);
        lastGPS.remove(id);
    }

}