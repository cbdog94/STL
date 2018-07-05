package web.bean;

/**
 * @author Bin Cheng
 */
public class TrajectoryInfo {
    public String taxiId;
    public double score;
    public boolean normal;
    public Object[] trajectory;

    public TrajectoryInfo(String taxiId, double score, boolean normal, Object[] trajectory) {
        this.taxiId = taxiId;
        this.score = score;
        this.normal = normal;
        this.trajectory = trajectory;
    }
}
