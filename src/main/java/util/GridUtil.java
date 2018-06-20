package util;

import bean.Cell;
import bean.GPS;

import java.util.ArrayList;
import java.util.List;

/**
 * GridUtil mainly used for meshing the GPS trajectory.
 *
 * @author Bin Cheng
 */
public class GridUtil {

    /**
     * Convert GPS Sequence into a tile Sequence,
     * through method of the Mercator projection mentioned in util.TileSystem.
     *
     * @param gpsList GPS Sequence
     * @return tile Sequence
     */
    public static List<Cell> gridGPSSequence(List<GPS> gpsList) {

        List<Cell> cellList = new ArrayList<>();
        if (gpsList == null || gpsList.size() == 0) {
            return cellList;
        }

        Cell lastCell = TileSystem.gpsToTile(gpsList.get(0));
        cellList.add(lastCell);
        //TODO 可能存在漂移的点，需要过滤掉
        for (int i = 1; i < gpsList.size(); i++) {
            Cell currentCell = TileSystem.gpsToTile(gpsList.get(i));
            //cellList should't have duplicate tiles
            if (!TileSystem.equal(lastCell,currentCell)) {
                cellList.addAll(addVirtualTiles(lastCell, currentCell));
            }

            lastCell = currentCell;
        }
        return cellList;
    }

    /**
     * Two title in the title sequence may not be adjacent,
     * in order to make the title sequence continues,
     * this method will add virtual tile points between start title and end title.
     *
     * @param startCell the start tile
     * @param endCell   the end tile
     * @return the virtual tiles that between start tile and end tile, included end tile
     */
    private static List<Cell> addVirtualTiles(Cell startCell, Cell endCell) {

        List<Cell> virtualList = new ArrayList<>();
        int tmpX = startCell.getTileX();
        int tmpY = startCell.getTileY();
        int endX = endCell.getTileX();
        int endY = endCell.getTileY();
        while (tmpX != endX || tmpY != endY) {
            int gradientX = endX - tmpX;
            int gradientY = endY - tmpY;
            if (gradientX > 0) {
                tmpX++;
            } else if (gradientX < 0) {
                tmpX--;
            }
            if (gradientY > 0) {
                tmpY++;
            } else if (gradientY < 0) {
                tmpY--;
            }
            virtualList.add(new Cell(tmpX, tmpY));
        }
        return virtualList;
    }

}
