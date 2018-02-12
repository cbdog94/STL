import bean.GPS;
import com.google.gson.Gson;
import com.graphhopper.GraphHopper;
import com.graphhopper.matching.*;
import com.graphhopper.reader.osm.GraphHopperOSM;
import com.graphhopper.routing.AlgorithmOptions;
import com.graphhopper.routing.util.CarFlagEncoder;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.weighting.FastestWeighting;
import com.graphhopper.routing.weighting.Weighting;
import com.graphhopper.util.GPXEntry;
import com.graphhopper.util.InstructionList;
import com.graphhopper.util.Parameters;
import mapreduce.DailyTrajectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by cbdog94 on 2017/4/18.
 */
public class MapMatchingTest {
    public static void main(String[] args) throws Exception {
        // import OpenStreetMap data
        GraphHopper hopper = new GraphHopperOSM();
        hopper.setDataReaderFile("/Users/cbdog94/Downloads/shanghai_china.osm.pbf");
        hopper.setGraphHopperLocation("./target/mapmatchingtest");
        CarFlagEncoder encoder = new CarFlagEncoder();
        hopper.setEncodingManager(new EncodingManager(encoder));
        hopper.getCHFactoryDecorator().setEnabled(false);
        hopper.importOrLoad();

        // create MapMatching object, can and should be shared accross threads
        String algorithm = Parameters.Algorithms.DIJKSTRA_BI;
        Weighting weighting = new FastestWeighting(encoder);
        AlgorithmOptions algoOptions = new AlgorithmOptions(algorithm, weighting);
        MapMatching mapMatching = new MapMatching(hopper, algoOptions);
        mapMatching.setMeasurementErrorSigma(35.0);


        BufferedReader reader = new BufferedReader(new FileReader(new File("/Library/WebServer/Documents/used/30040.json")));
        String content = reader.readLine();
        reader.close();
//        System.out.println(content);
        DailyTrajectory.Trajectory trajectory = new Gson().fromJson(content, DailyTrajectory.Trajectory.class);
//        System.out.println(trajectory.taxiId);

        List<GPXEntry> points = new ArrayList<>();
//        List<List<PointInfo>> trajectories = new ArrayList<>();
        boolean flag = false;
        for (DailyTrajectory.PointInfo point : trajectory.points) {
            if (point.status == 0) {
                points.add(new GPXEntry(point.lat, point.lng, point.timestamp.getTime()));
                flag = true;
            } else if (point.status == 1 && flag) {
//                trajectories.add(points);
                MatchResult mr = mapMatching.doWork(points);
                new GPXFile(mr, new InstructionList(null)).doExport("./target/" + System.currentTimeMillis() + ".gpx");
                print(mr);
                points = new ArrayList<>();
                flag = false;
            }
        }
//        System.out.println(trajectories.size());
//        for (List<PointInfo> t : trajectories) {
//            extract(t);
//        }
//        System.out.println(new Gson().toJson(trajectories));
        // do the actual matching, get the GPX entries from a file or via stream
//        List<GPXEntry> inputGPXEntries = new GPXFile().doImport("nice.gpx").getEntries();
//        MatchResult mr = mapMatching.doWork(inputGPXEntries);
//
//        // return GraphHopper edges with all associated GPX entries
//        List<EdgeMatch> matches = mr.getEdgeMatches();
//        // now do something with the edges like storing the edgeIds or doing fetchWayGeometry etc
//        matches.get(0).getEdgeState();
    }

    private static void print(MatchResult mr) {
//         return GraphHopper edges with all associated GPX entries
        List<EdgeMatch> matches = mr.getEdgeMatches();
        List<GPS> points = new ArrayList<>();

        for (EdgeMatch edge : matches) {
//            System.out.println(edge.getGpxExtensions().size());
            for (GPXExtension gpXExtension : edge.getGpxExtensions()) {
//                System.out.println(gpXExtension);
                GPXEntry gpxEntry = gpXExtension.getEntry();
                points.add(new GPS(gpxEntry.getLat(), gpxEntry.getLon(), new Date(gpxEntry.getTime())));
            }
//            GPXEntry gpxEntry=edge.getGpxExtensions().get(0).getEntry();
//            points.add(gpxEntry);
        }

        System.out.println(new Gson().toJson(points));
        System.out.println("-----------");
    }
}
