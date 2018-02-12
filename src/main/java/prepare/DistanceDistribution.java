package prepare;

import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Created by cbdog94 on 2017/4/18.
 */
public class DistanceDistribution {

    public static void main(String[] args) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(new File("/Users/cbdog94/Downloads/part-r-00000")));
        String content ;

        int[] dist = new int[5000];
        double max = 0;
        double min = 1000;
        System.out.println();
        while ((content = reader.readLine()) != null) {

            double distance = Double.parseDouble(content.split("\t")[1]);

            max=Math.max(max,distance);
            min=Math.min(min,distance);
            dist[(int) distance / 100]++;
        }
        reader.close();
        System.out.println("Max distance:"+max);
        System.out.println("Min distance:"+min);
        System.out.println(new Gson().toJson(dist));

    }
}
