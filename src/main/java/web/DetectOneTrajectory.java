package web;

import algorithm.STLOpDetection;
import bean.Cell;
import bean.GPS;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import hbase.HBaseUtil;
import hbase.TrajectoryUtil;
import org.apache.commons.io.FileUtils;
import util.CommonUtil;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 可视化一条轨迹
 */
public class DetectOneTrajectory extends HttpServlet {


    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String id = request.getParameter("id");
        String start = request.getParameter("start");
        String end = request.getParameter("end");
        if (id == null || id.equals("") || start == null || start.equals("") || end == null || end.equals("")) {
            request.setAttribute("error", "Please input id, start-cell and end-cell!");
            response.setStatus(400);
            request.getRequestDispatcher("error.jsp").forward(request, response);
            return;
        }

        Cell startCell = new Cell(start), endCell = new Cell(end);

        // Origin trajectory.
        Map<String, List<GPS>> trajectoryGPS = TrajectoryUtil.getAllTrajectoryGPSs(startCell, endCell, "SH");
        System.out.println("trajectoryGPS size: " + trajectoryGPS.size());

        // Trajectory info.
        Map<String, double[]> trajectoryInfoMap = Maps.transformValues(trajectoryGPS, s -> CommonUtil.trajectoryInfo(s, "SH"));


        // Generate training set.
        double[] distArray = trajectoryInfoMap.values().stream().mapToDouble(s -> s[0]).toArray();
        double[] timeArray = trajectoryInfoMap.values().stream().mapToDouble(s -> s[1]).toArray();

        double distance60 = CommonUtil.percentile(distArray, 0.6);
        double time60 = CommonUtil.percentile(timeArray, 0.6);

        Map<String, List<GPS>> trainTrajectory = Maps.filterKeys(trajectoryGPS, Maps.filterValues(trajectoryInfoMap, s -> s[0] < distance60 && s[1] < time60)::containsKey);
        System.out.println("Train set size:" + trainTrajectory.size());

        //Detect
        List<GPS> testTrajectory = trajectoryGPS.get(id);
        System.out.println(testTrajectory.get(0));
        System.out.println(testTrajectory.get(testTrajectory.size() - 1));

        List<STLOpDetection.AnomalyInfo> anomalyInfoList = new ArrayList<>(testTrajectory.size());
        new STLOpDetection().detect(new ArrayList<>(trainTrajectory.values()), testTrajectory, anomalyInfoList, true);

        String filename = "anomaly_" + id + "_" + start.substring(1, start.length() - 1).replace(',', '_') + "_" + end.substring(1, end.length() - 1).replace(',', '_') + ".json";

        //output
        String root = getServletContext().getRealPath("/");
        File file = FileUtils.getFile(root + filename);
        String content = new Gson().toJson(anomalyInfoList);
        try {
            FileUtils.write(file, content, "UTF-8", false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        request.setAttribute("filename", filename);
        request.getRequestDispatcher("detectOneTrajectory.jsp").forward(request, response);
    }


    @Override
    public void destroy() {
        System.out.println("destroy");
        HBaseUtil.close();
        super.destroy();
    }
}