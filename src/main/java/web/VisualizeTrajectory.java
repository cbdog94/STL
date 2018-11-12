package web;

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
import java.util.*;

/**
 * 可视化一条轨迹
 */
public class VisualizeTrajectory extends HttpServlet {


    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String hour = request.getParameter("hour");
        String start = request.getParameter("start");
        String end = request.getParameter("end");
        if (hour == null || hour.equals("") || start == null || start.equals("") || end == null || end.equals("")) {
            request.setAttribute("error", "Please input hour, start-cell and end-cell!");
            response.setStatus(400);
            request.getRequestDispatcher("error.jsp").forward(request, response);
            return;
        }

        Cell startCell = new Cell(start), endCell = new Cell(end);
        int startHour = Integer.parseInt(hour);
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


        Calendar calendar = GregorianCalendar.getInstance();

        Map<String, List<GPS>> filterTrajectory = Maps.filterValues(trainTrajectory, x -> {
            calendar.setTime(x.get(0).getTimestamp());
            int currentStartHour = calendar.get(Calendar.HOUR_OF_DAY);
            return currentStartHour >= startHour - 1 &&
                    currentStartHour <= startHour + 1;
        });

        String content = new Gson().toJson(web.CommonUtil.multiCompress(new ArrayList<>(filterTrajectory.values())));
        String filename = "history_" + hour + "_" + start.substring(1, start.length() - 1).replace(',', '_') + "_" + end.substring(1, end.length() - 1).replace(',', '_') + ".json";

        //output
        String root = getServletContext().getRealPath("/");
        File file = FileUtils.getFile(root + filename);
        try {
            FileUtils.write(file, content, "UTF-8", false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        request.setAttribute("filename", filename);
        request.getRequestDispatcher("visualTrajectory.jsp").forward(request, response);
    }


    @Override
    public void destroy() {
        System.out.println("destroy");
        HBaseUtil.close();
        super.destroy();
    }
}