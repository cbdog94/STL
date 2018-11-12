<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<!DOCTYPE HTML>
<html>

<head>
    <title>单条轨迹</title>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
    <meta name="viewport"
          content="width=device-width, initial-scale=1.0, maximum-scale=1.0, minimum-scale=1.0, user-scalable=no">
    <style type="text/css">
        html,
        body {
            margin: 0;
            width: 100%;
            height: 100%;
            background: #ffffff;
        }

        #map {
            width: 100%;
            height: 100%;
        }

    </style>
    <script type="text/javascript"
            src="http://api.map.baidu.com/api?v=3.0&ak=TGVn20r8ILbAE5iLGVfGUmwqIoPBenya"></script>
    <script src="http://cdn.bootcss.com/jquery/3.3.1/jquery.min.js"></script>
    <script type="text/javascript" src="tool.js"></script>
</head>

<body>
<div id="map"></div>
<script type="text/javascript">
    var map = new BMap.Map("map", {});                        // 创建Map实例
    map.centerAndZoom(new BMap.Point(121.592292, 31.244127), 10);     // 初始化地图,设置中心点坐标和地图级别
    map.enableScrollWheelZoom();                        //启用滚轮放大缩小
    if (document.createElement('canvas').getContext) {  // 判断当前浏览器是否支持绘制海量点
        $.get('${filename}', function (json) {
            json.map(function (point) {
                var baiduPoint = wgs2bd(point.latitude, point.longitude);
                var marker = new BMap.Marker(new BMap.Point(baiduPoint[1], baiduPoint[0]));  // 创建标注
                map.addOverlay(marker);              // 将标注添加到地图中

                var label = new BMap.Label(point.timestamp, {offset: new BMap.Size(20, -10)});
                marker.setLabel(label);
            });
        });
    } else {
        alert('请在chrome、safari、IE8+以上浏览器查看本示例');
    }
</script>
</body>

</html>
