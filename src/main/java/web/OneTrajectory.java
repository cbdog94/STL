package web;

import bean.Cell;
import bean.GPS;
import com.google.gson.Gson;
import hbase.HBaseUtil;
import hbase.TrajectoryUtil;
import org.apache.commons.io.FileUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * 可视化一条轨迹
 */
public class OneTrajectory extends HttpServlet {


    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String id = request.getParameter("id");
        if (id == null || id.equals("")) {
            request.setAttribute("error", "Please input id!");
            response.setStatus(400);
            request.getRequestDispatcher("error.jsp").forward(request, response);
            return;
        }
        String start = request.getParameter("start");
        String end = request.getParameter("end");
        String filename = "trajectory_" + id;

        List<GPS> trajectory = TrajectoryUtil.getTrajectoryGPSPoints(id);
        if (start != null && !start.equals("") && end != null && !end.equals("")) {
            trajectory = TrajectoryUtil.removeExtraGPS(trajectory, new Cell(start), new Cell(end));
            filename += ("_" + start + "_" + end);
        }
        filename += ".json";

        //output
        String root = getServletContext().getRealPath("/");
        File file = FileUtils.getFile(root + filename);
        String content = new Gson().toJson(trajectory);
        try {
            FileUtils.write(file, content, "UTF-8", false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        request.setAttribute("filename", filename);
        request.getRequestDispatcher("oneTrajectory.jsp").forward(request, response);
    }


    @Override
    public void destroy() {
        System.out.println("destroy");
        HBaseUtil.close();
        super.destroy();
    }
}