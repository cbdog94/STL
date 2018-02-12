package bean;

import util.CommonUtil;
import util.TileSystem;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Cell point
 *
 * @author Bin Cheng
 */
public class Cell implements Serializable{

    private int tileX;
    private int tileY;

    public int getTileX() {
        return tileX;
    }

    public void setTileX(int tileX) {
        this.tileX = tileX;
    }

    public int getTileY() {
        return tileY;
    }

    public void setTileY(int tileY) {
        this.tileY = tileY;
    }

    public Cell(int tileX, int tileY) {
        this.tileX = tileX;
        this.tileY = tileY;
    }

    /***
     * @param tile eg:[219547,107093]
     */
    public Cell(String tile) {
        try {
            Matcher matcher = Pattern.compile("\\[(\\w+),(\\w+)]").matcher(tile);
            if (!matcher.lookingAt())
                return;
            this.tileX = Integer.parseInt(matcher.group(1));
            this.tileY = Integer.parseInt(matcher.group(2));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Cell && tileX == ((Cell) obj).tileX && tileY == ((Cell) obj).tileY;
    }

    @Override
    public String toString() {
        return "[" + tileX + "," + tileY + "]";
    }

    public double distance(Cell cell) {
        GPS current = TileSystem.TileToGPS(this);
        GPS compare = TileSystem.TileToGPS(cell);

        return CommonUtil.distanceBetween(current, compare);
    }

    public static void main(String[] args) {
        Cell cell = new Cell("[109776,53554]");
        Cell cell1 = new Cell("[109776,53553]");
        System.out.println(cell.distance(cell1));
    }
}
