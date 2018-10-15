# STL: Online Detection of Taxi Trajectory Anomaly based on Spatial-Temporal Laws [![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](https://github.com/your/your-project/blob/master/LICENSE)

This project implements some taxi trajectory anomaly detection methods, including [iBAT](http://cs.nju.edu.cn/zhouzh/zhouzh.files/publication/ubicomp11.pdf), [iBOAT](http://pgembeddedsystems.com/download/java/2013-IEEE%20iBOAT%20Isolation-Based%20Online%20Anomalous%20Trajectory%20Detection.pdf), [OnATrade](http://or.nsfc.gov.cn/bitstream/00001903-5/481350/1/9913465546.pdf) and my proposed STL.

## Getting started
The trajectory anomaly detection consists of two part: trajectory pre-processing and online detection. Trajectory pre-processing converts the raw trajectory records into structured data. Online detection uses the processed data to detect the incoming trajectory.

In this project, the raw trajectory record is stored in [Baidu Netdisk](https://pan.baidu.com/s/1FlNO1CfXiyi15WLuXn_ucA) (code:*w2uu*), more detail of the which can be found in [Dataset](Dataset).

## Dataset
* **sh_taxi_data**
    + Collected from Shanghai, China during Apr., 2015.
    + Field description: taxi ID, alarm, **empty**, ceiling light status, ?, brake, receive time, GPS time, longitude, latitude, speed, direction, #satellites
* **sz_taxi_data**
    + Collected from Shenzhen, Guangdong, China during Sep., 2009.
    + Field description: taxi ID, time, longitude, latitude, speed, direction, **occupied**   
* **sh_taxi_data**
    + Collected from Chengdu, Sichuan, China during Aug., 2014.
    + Field description: taxi ID, latitude, longitude, **occupied**, time
    
## Demo
[STL Demo](http://stl.cbdog94.cn:18080/STL/)  