package constant;

import bean.Cell;

/**
 * Created by cbdog94 on 2017/4/21.
 */
public class DetectConstant {

    //1000+
    //1248 76
    public static Cell startPoint = new Cell("[109776,53554]");//陆家嘴
    public static Cell endPoint = new Cell("[109873,53574]");//浦东机场

    private static String[] startPoints = {
            "[109775,53554]", "[109775,53554]", "[109752,53522]", "[109752,53522]", "[109752,53522]",
            "[109752,53522]", "[109752,53522]", "[109752,53522]", "[109752,53522]", "[109752,53522]",
            "[109752,53522]", "[109752,53522]", "[109752,53522]", "[109752,53522]", "[109752,53522]"};
    private static String[] endPoints = {
            "[109873,53574]", "[109793,53562]", "[109724,53547]", "[109711,53558]", "[109729,53567]",
            "[109763,53539]", "[109740,53536]", "[109728,53572]", "[109736,53504]", "[109723,53574]",
            "[109765,53580]", "[109740,53538]", "[109728,53571]", "[109767,53556]", "[109736,53536]"};

    public static Cell[] getCellPoint(int index) {
        return new Cell[]{new Cell(startPoints[index]), new Cell(endPoints[index])};
    }

    //1749 111
//    public static Cell startPoint = new Cell("[109775,53554]");//陆家嘴
//    public static Cell endPoint = new Cell("[109793,53562]");//

    //1216 78
//    public static Cell startPoint = new Cell("[109752,53522]");
//    public static Cell endPoint = new Cell("[109724,53547]");

    //1705 83
//    public static Cell startPoint = new Cell("[109752,53522]");//
//    public static Cell endPoint = new Cell("[109711,53558]");//

    //1143 62
//    public static Cell startPoint = new Cell("[109752,53522]");//
//    public static Cell endPoint = new Cell("[109729,53567]");//

    //200+
    //310 16
//    public static Cell startPoint = new Cell("[109752,53522]");//
//    public static Cell endPoint = new Cell("[109763,53539]");//

    //358 24
//    public static Cell startPoint = new Cell("[109752,53522]");//
//    public static Cell endPoint = new Cell("[109740,53536]");//

    //333 32
//    public static Cell startPoint = new Cell("[109752,53522]");//
//    public static Cell endPoint = new Cell("[109728,53572]");//

    //384 31
//    public static Cell startPoint = new Cell("[109752,53522]");//
//    public static Cell endPoint = new Cell("[109736,53504]");//

    //329 32
//    public static Cell startPoint = new Cell("[109752,53522]");//
//    public static Cell endPoint = new Cell("[109723,53574]");//

//50+
    //130 9
//    public static Cell startPoint = new Cell("[109752,53522]");//
//    public static Cell endPoint = new Cell("[109765,53580]");//

    //146 11
//    public static Cell startPoint = new Cell("[109752,53522]");//
//    public static Cell endPoint = new Cell("[109740,53538]");//

    //135 11
//    public static Cell startPoint = new Cell("[109752,53522]");//
//    public static Cell endPoint = new Cell("[109728,53571]");//

    //124 9
//    public static Cell startPoint = new Cell("[109752,53522]");//
//    public static Cell endPoint = new Cell("[109767,53556]");//

    //140 11
//    public static Cell startPoint = new Cell("[109752,53522]");//
//    public static Cell endPoint = new Cell("[109736,53536]");//


    //    public static Cell startPoint = new Cell("[109776,53525]");//五角场
//    public static Cell endPoint = new Cell("[109756,53546]");//上海站

//    public static Cell startPoint = new Cell("[109776,53525]");//五角场
//    public static Cell endPoint = new Cell("[109875,53576]");//浦东机场

    //400+
//    public static Cell startPoint = new Cell("[109776,53554]");//陆家嘴
//    public static Cell endPoint = new Cell("[109745,53586]");//上海南

//    public static Cell startPoint = new Cell("[109807,53566]");//张江
//    public static Cell endPoint = new Cell("[109754,53549]");//上海站

    public static String outputPath = "/Library/WebServer/Documents/used/";
}
