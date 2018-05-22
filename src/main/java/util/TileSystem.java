package util;

import bean.Cell;
import bean.GPS;

/**
 * The implementation of Mercator projection
 *
 * @see <a href="https://msdn.microsoft.com/en-us/library/bb259689.aspx">Bing Maps Tile System</a>
 */
public class TileSystem {
    private static final double EARTH_RADIUS = 6378137;
    private static final double MIN_LATITUDE = -85.05112878;
    private static final double MAX_LATITUDE = 85.05112878;
    private static final double MIN_LONGITUDE = -180;
    private static final double MAX_LONGITUDE = 180;

    private static final int DEFAULT_LEVEL = 17;


    /**
     * Clips a number to the specified minimum and maximum values.
     *
     * @param n        The number to clip.
     * @param minValue Minimum allowable value.
     * @param maxValue Maximum allowable value.
     * @return The clipped value.
     */
    private static double clip(double n, double minValue, double maxValue) {
        return Math.min(Math.max(n, minValue), maxValue);
    }


    /**
     * Determines the map width and height (in pixels) at a specified level of detail.
     *
     * @param levelOfDetail Level of detail, from 1 (lowest detail) to 23 (highest detail).
     * @return The map width and height in pixels.
     */
    public static long mapSize(int levelOfDetail) {
        return 256 << levelOfDetail;
    }


    /**
     * Determines the ground resolution (in meters per pixel) at a specified
     * latitude and level of detail.
     *
     * @param latitude      Latitude (in degrees) at which to measure the ground resolution.
     * @param levelOfDetail Level of detail, from 1 (lowest detail) to 23 (highest detail).
     * @return The ground resolution, in meters per pixel.
     */
    private static double groundResolution(double latitude, int levelOfDetail) {
        latitude = clip(latitude, MIN_LATITUDE, MAX_LATITUDE);
        return Math.cos(latitude * Math.PI / 180) * 2 * Math.PI * EARTH_RADIUS / mapSize(levelOfDetail);
    }


    /**
     * Determines the map scale at a specified latitude, level of detail,
     * and screen resolution.
     *
     * @param latitude      Latitude (in degrees) at which to measure the map scale.
     * @param levelOfDetail Level of detail, from 1 (lowest detail) to 23 (highest detail).
     * @param screenDpi     Resolution of the screen, in dots per inch.
     * @return The map scale, expressed as the denominator N of the ratio 1 : N.
     */
    public static double mapScale(double latitude, int levelOfDetail, int screenDpi) {
        return groundResolution(latitude, levelOfDetail) * screenDpi / 0.0254;
    }


    /**
     * Converts a point from latitude/longitude WGS-84 coordinates (in degrees)
     * into pixel XY coordinates at a specified level of detail.
     *
     * @param latitude      Latitude of the point, in degrees.
     * @param longitude     Longitude of the point, in degrees.
     * @param levelOfDetail Level of detail, from 1 (lowest detail) to 23 (highest detail).
     * @return an array of PixelXY, index 0 is PixelX, index 1 is PixelY
     */
    public static int[] latLongToPixelXY(double latitude, double longitude, int levelOfDetail) {
        latitude = clip(latitude, MIN_LATITUDE, MAX_LATITUDE);
        longitude = clip(longitude, MIN_LONGITUDE, MAX_LONGITUDE);

        double x = (longitude + 180) / 360;
        double sinLatitude = Math.sin(latitude * Math.PI / 180);
        double y = 0.5 - Math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI);

        long mapSize = mapSize(levelOfDetail);
        int pixelX = (int) clip(x * mapSize + 0.5, 0, mapSize - 1);
        int pixelY = (int) clip(y * mapSize + 0.5, 0, mapSize - 1);

        return new int[]{pixelX, pixelY};
    }


    /**
     * Converts a pixel from pixel XY coordinates at a specified level of detail
     * into latitude/longitude WGS-84 coordinates (in degrees).
     *
     * @param pixelX        X coordinate of the point, in pixels.
     * @param pixelY        Y coordinates of the point, in pixels.
     * @param levelOfDetail Level of detail, from 1 (lowest detail) to 23 (highest detail).
     * @return an array of LatLong, index 0 is latitude, index 1 is longitude
     */
    public static double[] pixelXYToLatLong(int pixelX, int pixelY, int levelOfDetail) {
        double mapSize = mapSize(levelOfDetail);
        double x = (clip(pixelX, 0, mapSize - 1) / mapSize) - 0.5;
        double y = 0.5 - (clip(pixelY, 0, mapSize - 1) / mapSize);

        double latitude = 90 - 360 * Math.atan(Math.exp(-y * 2 * Math.PI)) / Math.PI;
        double longitude = 360 * x;

        return new double[]{latitude, longitude};
    }


    /**
     * Converts pixel XY coordinates into tile XY coordinates of the tile containing
     * the specified pixel.
     *
     * @param pixelX Pixel X coordinate.
     * @param pixelY Pixel Y coordinate.
     * @return an array of TileXY, index 0 is tileX, index 1 is tileY
     */
    public static int[] pixelXYToTileXY(int pixelX, int pixelY) {
        int tileX = pixelX / 256;
        int tileY = pixelY / 256;
        return new int[]{tileX, tileY};
    }


    /**
     * Converts tile XY coordinates into tile XY coordinates of the upper-left pixel
     * of the specified tile.
     *
     * @param tileX Cell X coordinate.
     * @param tileY Cell Y coordinate.
     * @return an array of PixelXY, index 0 is pixelX, index 1 is pixelY
     */
    public static int[] tileXYToPixelXY(int tileX, int tileY) {
        int pixelX = tileX * 256;
        int pixelY = tileY * 256;
        return new int[]{pixelX, pixelY};
    }

    /**
     * Converts a point from latitude/longitude WGS-84 coordinates (in degrees)
     * into tile XY coordinates at a specified level of detail.
     *
     * @param latitude  Latitude of the point, in degrees.
     * @param longitude Longitude of the point, in degrees.
     * @return an array of TileXY, index 0 is TileX, index 1 is TileY
     */
    public static int[] latLongToTileXY(double latitude, double longitude) {
        int[] pixelXY = latLongToPixelXY(latitude, longitude, DEFAULT_LEVEL);
        return pixelXYToTileXY(pixelXY[0], pixelXY[1]);
    }


    public static Cell GPSToTile(GPS GPSPoint) {
        int[] pixelXY = latLongToPixelXY(GPSPoint.getLatitude(), GPSPoint.getLongitude(), DEFAULT_LEVEL);
        int[] tileXY = pixelXYToTileXY(pixelXY[0], pixelXY[1]);
        return new Cell(tileXY[0], tileXY[1]);
    }

    public static GPS TileToGPS(Cell cell) {
        int[] pixelXY = tileXYToPixelXY(cell.getTileX(), cell.getTileY());
        double[] GPSXY = pixelXYToLatLong(pixelXY[0], pixelXY[1], DEFAULT_LEVEL);
        return new GPS(GPSXY[0], GPSXY[1], null);
    }


    /**
     * Converts tile XY coordinates into a QuadKey at a specified level of detail.
     *
     * @param tileX         Cell X coordinate.
     * @param tileY         Cell Y coordinate.
     * @param levelOfDetail Level of detail, from 1 (lowest detail) to 23 (highest detail).
     * @return A string containing the QuadKey.
     */
    public static String tileXYToQuadKey(int tileX, int tileY, int levelOfDetail) {
        StringBuilder quadKey = new StringBuilder();
        for (int i = levelOfDetail; i > 0; i--) {
            char digit = '0';
            int mask = 1 << (i - 1);
            if ((tileX & mask) != 0) {
                digit++;
            }
            if ((tileY & mask) != 0) {
                digit++;
                digit++;
            }
            quadKey.append(digit);
        }
        return quadKey.toString();
    }


    /**
     * Converts a QuadKey into tile XY coordinates.
     *
     * @param quadKey QuadKey of the tile.
     * @return an array of result, index 0 is tileX, index 1 is tileY, index 2 is levelOfDetail
     */
    public static int[] quadKeyToTileXY(String quadKey) {
        int tileX = 0;
        int tileY = 0;
        int levelOfDetail = quadKey.length();
        for (int i = levelOfDetail; i > 0; i--) {
            int mask = 1 << (i - 1);
            switch (quadKey.charAt(levelOfDetail - i)) {
                case '0':
                    break;

                case '1':
                    tileX |= mask;
                    break;

                case '2':
                    tileY |= mask;
                    break;

                case '3':
                    tileX |= mask;
                    tileY |= mask;
                    break;

                default:
                    throw new IllegalArgumentException("Invalid QuadKey digit sequence.");
            }
        }
        return new int[]{tileX, tileY, levelOfDetail};
    }

}

