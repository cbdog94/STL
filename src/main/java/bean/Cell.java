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
            if (!matcher.lookingAt()) {
                return;
            }
            this.tileX = Integer.parseInt(matcher.group(1));
            this.tileY = Integer.parseInt(matcher.group(2));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "[" + tileX + "," + tileY + "]";
    }

}
