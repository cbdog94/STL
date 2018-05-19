package bean;

import java.io.Serializable;

public class Section implements Serializable {

    private int internalId;
    private long wayId;
    private String highWay;
    private double distance;

    public int getInternalId() {
        return internalId;
    }

    public void setInternalId(int internalId) {
        this.internalId = internalId;
    }

    public long getWayId() {
        return wayId;
    }

    public void setWayId(long wayId) {
        this.wayId = wayId;
    }

    public String getHighWay() {
        return highWay;
    }

    public void setHighWay(String highWay) {
        this.highWay = highWay;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public Section(int internalId, long wayId, String highWay, double distance) {
        this.internalId = internalId;
        this.wayId = wayId;
        this.highWay = highWay;
        this.distance = distance;
    }

    @Override
    public String toString() {
        return "Section{" +
                "internalId=" + internalId +
                ", wayId=" + wayId +
                ", highWay='" + highWay + '\'' +
                ", distance=" + distance +
                '}';
    }
}
