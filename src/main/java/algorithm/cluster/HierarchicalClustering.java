package algorithm.cluster;

import clustering.Cluster;
import clustering.ClusteringAlgorithm;
import clustering.CompleteLinkageStrategy;
import clustering.DefaultClusteringAlgorithm;
import org.apache.commons.math3.stat.StatUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HierarchicalClustering implements ClusterUtil {

    private int[] map;
    private List<List<Integer>> clusterHour;
//    private final double THRESHOLD = 0.3;

    public HierarchicalClustering(double[] timeList, double threshold) {
        map = new int[24];
        clusterHour = new ArrayList<>();
        timeList = StatUtils.normalize(timeList);

        String[] names = new String[24];
        double[][] distances = new double[24][24];

        for (int i = 0; i < 24; i++) {
            names[i] = i + "";
            for (int j = 0; j < 24; j++) {
                distances[i][j] = Math.abs(timeList[i] - timeList[j]);
            }
        }

        ClusteringAlgorithm alg = new DefaultClusteringAlgorithm();
        List<Cluster> clusters = alg.performFlatClustering(distances, names,
                new CompleteLinkageStrategy(), threshold);
        for (int i = 0; i < clusters.size(); i++) {
            List<Integer> t = getAllPoints(clusters.get(i));
            for (Integer point : t) {
                map[point] = i;
            }
            clusterHour.add(t);
        }
        System.out.println("cluster size" + clusters.size());
    }

    private static List<Integer> getAllPoints(Cluster cluster) {
        if (cluster.getChildren().size() == 0) {
            return Collections.singletonList(Integer.valueOf(cluster.getName()));
        }
        List<Integer> result = new ArrayList<>();
        for (Cluster c : cluster.getChildren()) {
            result.addAll(getAllPoints(c));
        }
        return result;
    }

    @Override
    public List<List<Integer>> getAllClusters() {
        return clusterHour;
    }

    @Override
    public int[] getClusterMap() {
        return map;
    }
}
