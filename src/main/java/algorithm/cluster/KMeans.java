package algorithm.cluster;

import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.commons.math3.stat.StatUtils;

import java.util.ArrayList;
import java.util.List;

public class KMeans implements ClusterUtil {

    private int[] map;
    private List<List<Integer>> clusterHour;

    public KMeans(double[] timeList, int K) {
        map = new int[24];
        clusterHour = new ArrayList<>();
        timeList = StatUtils.normalize(timeList);

        List<Period> list = new ArrayList<>(24);
        for (int i = 0; i < 24; i++) {
            list.add(new Period(i, timeList[i]));
        }
        KMeansPlusPlusClusterer<Period> kmeans = new KMeansPlusPlusClusterer<>(K, 10000);
        List<CentroidCluster<Period>> clusterResult = kmeans.cluster(list);
        for (int i = 0; i < clusterResult.size(); i++) {
            CentroidCluster<Period> cluster = clusterResult.get(i);
            List<Integer> item = new ArrayList<>();
            for (Period p : cluster.getPoints()) {
                item.add(p.period);
                map[p.period] = i;
            }
            clusterHour.add(item);
        }
        System.out.println("cluster size" + K);
    }

    @Override
    public List<List<Integer>> getAllClusters() {
        return clusterHour;
    }

    @Override
    public int[] getClusterMap() {
        return map;
    }

    class Period implements Clusterable {

        int period;
        double time;

        Period(int period, double time) {
            this.period = period;
            this.time = time;
        }

        @Override
        public double[] getPoint() {
            return new double[]{time};
        }
    }
}
