import bean.Cell;
import bean.GPS;
import constant.DetectConstant;
import hbase.TrajectoryUtil;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SerializeCellPoint {


    public static void main(String[] args) {
        //write
//        for (int i = 0; i < 15; i++) {
//            Cell[] cells = DetectConstant.getCellPoint(i);
//            Map<String, List<Cell>> allCellTrajectories = TrajectoryUtil.getAllTrajectoryCells(cells[0], cells[1]);
//            Map<String, List<GPS>> allGPSTrajectories = new HashMap<>();
//            for (String id : allCellTrajectories.keySet()) {
//                allGPSTrajectories.put(id, TrajectoryUtil.getTrajectoryGPSPoints(id));
//            }
//            saveObjToFile(allCellTrajectories, "serializeFile/cells" + i);
//            saveObjToFile(allGPSTrajectories, "serializeFile/points" + i);
//            System.out.println("Finish " + i);
//        }

        //read
        Map<String, List<GPS>> map = (Map<String, List<GPS>>) getObjFromFile("serializeFile/points1");
        assert map != null;
        System.out.println(map.size());

    }

    private static void saveObjToFile(Object o, String fileName) {
        try {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(fileName));
            oos.writeObject(o);
            oos.close();                        //关闭文件流
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static Object getObjFromFile(String fileName) {
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileName));
            return ois.readObject();
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
