package test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test3 {
    public static void main(String[] args) {
        String traj = "[121.525775,31.287287,2015-04-13 23:57:35]";
//        Pattern.compile("\\s(\\d+):").matcher(traj).group(0)
//        System.out.println(Pattern.compile(".*\\s(\\d+):.*").matcher(traj).find());
        Matcher matcher = Pattern.compile(".*\\s(\\d+):.*").matcher(traj);
        if (matcher.find()) {
            System.out.println(matcher.group(1));
        }
    }
}
