package algorithm;

import algorithm.onatrade.GraphHopperWithHighway;
import algorithm.onatrade.OsmReaderWithHighway;
import bean.Cell;
import bean.GPS;
import bean.Section;
import com.graphhopper.GraphHopper;
import com.graphhopper.matching.GPXFile;
import com.graphhopper.matching.MapMatching;
import com.graphhopper.matching.MatchResult;
import com.graphhopper.routing.AlgorithmOptions;
import com.graphhopper.routing.util.CarFlagEncoder;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.util.FlagEncoder;
import com.graphhopper.routing.weighting.FastestWeighting;
import com.graphhopper.routing.weighting.Weighting;
import com.graphhopper.util.EdgeIteratorState;
import com.graphhopper.util.Parameters;
import crosby.binary.osmosis.OsmosisReader;
import util.CommonUtil;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * The algorithm of OnATrade.
 *
 * @author Bin Cheng
 */
public class OnATradeDetection {

    private Map<String, GPXFile> debugGPXFileMap;
    private boolean debug;

    public OnATradeDetection(boolean debug) {
        this.debug = debug;
        debugGPXFileMap = new ConcurrentHashMap<>();
    }

    private GraphHopperWithHighway initGraphHopper(String city) {
        GraphHopperWithHighway hopper = new GraphHopperWithHighway();
        hopper.setDataReaderFile("OSM/" + city + ".osm.pbf");
        hopper.setGraphHopperLocation("OSM_load_" + city);
        CarFlagEncoder encoder = new CarFlagEncoder();
        hopper.setEncodingManager(new EncodingManager(encoder));
        hopper.getCHFactoryDecorator().setEnabled(false);
        hopper.importOrLoad();
        return hopper;
    }

    private MapMatching initMapMatching(GraphHopper hopper, FlagEncoder encoder) {
        String algorithm = Parameters.Algorithms.DIJKSTRA_BI;
        Weighting weighting = new FastestWeighting(encoder);
        AlgorithmOptions algoOptions = new AlgorithmOptions(algorithm, weighting);
        return new MapMatching(hopper, algoOptions);
    }


    private Map<Long, String> initHighWayMap(String city) {
        InputStream inputStream;
        try {
            inputStream = new FileInputStream("OSM/" + city + ".osm.pbf");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
        OsmosisReader reader = new OsmosisReader(inputStream);
        OsmReaderWithHighway highwaySink = new OsmReaderWithHighway();
        reader.setSink(highwaySink);
        reader.run();
        return highwaySink.getHighway();
    }

    @SuppressWarnings("unchecked")
    public Map<String, List<Section>> generateAbstractTrajectory(Cell startCell, Cell endCell, Map<String, List<GPS>> trajectoryGPS, String city) {
        //Load cache
        Object cache = CommonUtil.getObjFromFile("cache/" + startCell + "_" + endCell);
        if (cache instanceof Map) {
            return (Map<String, List<Section>>) cache;
        }

        GraphHopperWithHighway hopper = initGraphHopper(city);
        MapMatching mm = initMapMatching(hopper, hopper.getEncodingManager().getEncoder("car"));
        Map<Long, String> highwayMap = initHighWayMap(city);

        Map<String, List<Section>> absTrajectory = new ConcurrentHashMap<>(trajectoryGPS.size());
        trajectoryGPS.forEach(
                (k, v) -> {
                    MatchResult mr = mm.doWork(v.stream().map(s -> s.convertToGPX(city)).collect(Collectors.toList()));
                    List<Section> sections = mr.getEdgeMatches().stream().map(s -> {
                        EdgeIteratorState edge = s.getEdgeState();
                        int edgeID = edge.getEdge();
                        return new Section(edgeID, hopper.getOSMWay(edgeID), highwayMap.get(hopper.getOSMWay(edgeID)), edge.getDistance());
                    }).collect(Collectors.toList());
                    absTrajectory.put(k, sections);
                    if (debug) {
                        debugGPXFileMap.put(k, new GPXFile(mr, null));
                    }
                }
        );
        CommonUtil.saveObjToFile(absTrajectory, "cache/" + startCell + "_" + endCell);
        return absTrajectory;
    }

    public Map<String, List<Section>> routeRecommend(Map<String, List<Section>> absTrajectory, int k, double thresholdSim, double thresholdDis) {
        Map<String, List<Section>> cddTrajectory = new HashMap<>(absTrajectory.size() / 2);
        Map<String, Integer> cddCount = new HashMap<>(absTrajectory.size() / 2);
        //Randomly choose one.
        Map.Entry<String, List<Section>> first = absTrajectory.entrySet().iterator().next();
        cddTrajectory.put(first.getKey(), first.getValue());
        cddCount.put(first.getKey(), 0);
        //Find the popular trajectory.
        Iterator<Map.Entry<String, List<Section>>> it = absTrajectory.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, List<Section>> item = it.next();
            similarity(item, cddTrajectory, cddCount, thresholdSim);
            it.remove();
        }
        //Sort desc.
        List<Map.Entry<String, Integer>> cddCountList = new ArrayList<>(cddCount.entrySet());
        cddCountList.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        //Compute distance.
        Map<String, Double> cddDist = new HashMap<>(cddCountList.size());
        Double minDist = Double.MAX_VALUE;
        for (Map.Entry<String, Integer> entry : cddCountList.subList(0, k)) {
            Double dist = cddTrajectory.get(entry.getKey()).parallelStream().map(Section::getDistance).reduce(0.0, Double::sum);
            minDist = Math.min(dist, minDist);
            cddDist.put(entry.getKey(), dist);
        }
        //Filter the trajectory is which too long.
        Map<String, List<Section>> result = new HashMap<>(cddDist.size());
        for (Map.Entry<String, Double> entry : cddDist.entrySet()) {
            if (entry.getValue() <= minDist * (1 + thresholdDis)) {
                result.put(entry.getKey(), cddTrajectory.get(entry.getKey()));
            }
            // Export the recommend trajectory.
            if (debug) {
                try {
                    debugGPXFileMap.get(entry.getKey()).doExport("debug/" + entry.getKey() + ".gpx");
                } catch (NullPointerException ignored) {
                }
            }
        }
        return result;
    }


    private void similarity(Map.Entry<String, List<Section>> testEntry, Map<String, List<Section>> cddTrajectory, Map<String, Integer> cddCount, double thresholdSim) {
        double maxSim = 0;
        String trajectoryID = null;
        for (Map.Entry<String, List<Section>> entry : cddTrajectory.entrySet()) {
            double sim = lcs(testEntry.getValue(), entry.getValue()) / (double) Math.min(testEntry.getValue().size(), entry.getValue().size());
            if (sim > maxSim) {
                maxSim = sim;
                trajectoryID = entry.getKey();
            }
        }
        if (maxSim < thresholdSim) {
            cddTrajectory.put(testEntry.getKey(), testEntry.getValue());
            cddCount.put(testEntry.getKey(), 1);
        } else {
            cddCount.put(trajectoryID, cddCount.get(trajectoryID) + 1);
        }
    }

    private int lcs(List<Section> t1, List<Section> t2) {
        int m = t1.size(), n = t2.size();
        int[][] c = new int[m + 1][n + 1];
        for (int i = 1; i <= m; i++) {
            for (int j = 1; j <= n; j++) {
                if (t1.get(i - 1).getInternalId() == t2.get(j - 1).getInternalId()) {
                    c[i][j] = c[i - 1][j - 1] + 1;
                } else if (c[i - 1][j] >= c[i][j - 1]) {
                    c[i][j] = c[i - 1][j];
                } else {
                    c[i][j] = c[i][j - 1];
                }
            }
        }
        return c[m][n];
    }

    /**
     * The implementation of OnATrade.
     *
     * @param testTrajectory      Testing trajectory
     * @param recommendTrajectory Recommend trajectory set
     * @param threshold           Threshold of similarity
     * @return the proportion of anomalous points
     */
    public double detection(List<Section> testTrajectory, Map<String, List<Section>> recommendTrajectory, double threshold) {
        int totalCount = testTrajectory.size(), anomalyCount = 0, index = 0;
        double anomalyScore = 0, tau = 0.43729;
        List<Section> tmpTrajectory = new ArrayList<>();
        for (Section sec : testTrajectory) {
            tmpTrajectory.add(sec);
            double maxSim = recommendTrajectory.values().stream()
                    .mapToDouble(s -> (lcs(s, tmpTrajectory) * 1.0 / tmpTrajectory.size()))
                    .max().orElse(0);
            //Anomaly.
            if (1 - maxSim > threshold) {
                anomalyCount++;
            }
            if (index == 0) {
                anomalyScore = 1 - maxSim;
            } else {
                anomalyScore = (1 - tau) * (1 - maxSim) + tau * anomalyScore;
            }
            index++;
        }
        if (debug) {
            System.out.println("Anomaly Score: " + anomalyScore);
        }
        return anomalyCount * 1.0 / totalCount;
    }

}
