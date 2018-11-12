package web;

import bean.GPS;
import com.google.gson.Gson;
import web.bean.Result;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Common utility for web app.
 *
 * @author Bin Cheng
 */
public class CommonUtil {

    static void response(HttpServletRequest req, HttpServletResponse resp, int code, String msg) {
        Result result = new Result(code, msg);
        resp.setCharacterEncoding("UTF-8");
        resp.setContentType("application/json; charset=utf-8");
        try (PrintWriter out = resp.getWriter()) {
            out.print(req.getParameter("callback") + "(" + new Gson().toJson(result) + ")");
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static Object[] singleCompress(List<GPS> trajectoriesGPS) {
        long lastLat = 0, lastLon = 0;
        Object[] compressed = new Object[trajectoriesGPS.size() * 2];

        int index = 0;
        for (GPS gps : trajectoriesGPS) {
            if (index != 0) {
                compressed[index] = (long) (gps.getLatitude() * 1e6) - lastLat;
                compressed[index + 1] = (long) (gps.getLongitude() * 1e6) - lastLon;
            } else {
                compressed[index] = (long) (gps.getLatitude() * 1e6);
                compressed[index + 1] = (long) (gps.getLongitude() * 1e6);
            }
            lastLat = (long) (gps.getLatitude() * 1e6);
            lastLon = (long) (gps.getLongitude() * 1e6);
            index += 2;
        }
        return compressed;
    }

    static List<Object[]> multiCompress(List<List<GPS>> trajectoriesGPS) {
        return trajectoriesGPS.parallelStream().map(CommonUtil::singleCompress).collect(Collectors.toList());
    }

}