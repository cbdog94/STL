package algorithm;

import bean.Cell;
import util.TileSystem;

import java.util.*;

/**
 * The algorithm of iBAT.
 *
 * @author Bin Cheng
 */
public class iBATDetection {

    private static Random random = new Random();

    /**
     * The implementation of iBAT.
     *
     * @param testTrajectory  the trajectory waiting for detection
     * @param allTrajectories all the trajectories that have the same start cell and the end cell as the <b>testTrajectory</b>
     * @param numOfTrials     the number of trail detecting the <b>testTrajectory</b>
     * @param subSampleSize   the size of sub-sample
     */
    public static double iBAT(List<Cell> testTrajectory, List<List<Cell>> allTrajectories, int numOfTrials, int subSampleSize) {
        int[] numOfIsolation = new int[numOfTrials];
        int limit = (int) Math.ceil(Math.log(subSampleSize) / Math.log(2));
        List<Cell> shuffledTrajectory = new ArrayList<>(testTrajectory);

        for (int i = 0; i < numOfTrials; i++) {
            Collections.shuffle(shuffledTrajectory);
            List<List<Cell>> subSampleTrajectories = getSubSampleTrajectories(subSampleSize, allTrajectories);
            for (Cell cell : shuffledTrajectory) {
                numOfIsolation[i]++;
                subSampleTrajectories.removeIf(trajectory -> {
                    for (Cell oneCell : trajectory) {
                        if (TileSystem.equal(oneCell, cell)) {
                            return false;
                        }
                    }
                    return true;
                });
                if (subSampleTrajectories.size() < limit) {
                    break;
                }
            }
        }
        return anomalyScore(numOfIsolation, subSampleSize);
    }


    /**
     * Get a sub-sample trajectories set from all the trajectories.
     */
    private static List<List<Cell>> getSubSampleTrajectories(int subSampleSize, List<List<Cell>> allTrajectories) {
        List<List<Cell>> subSampleTrajectories = new ArrayList<>();
        if (allTrajectories == null || allTrajectories.size() == 0) {
            return subSampleTrajectories;
        }

        int allTrajectoriesSize = allTrajectories.size();

        //In reality, this situation is rare.
        if (subSampleSize >= allTrajectoriesSize) {
            subSampleTrajectories.addAll(allTrajectories);
            return subSampleTrajectories;
        }

        Set<Integer> storeUsedIndex = new HashSet<>();

        int temp;
        for (int i = 0; i < subSampleSize; i++) {
            //Check whether the trajectory which the index is temp has been chosen.
            while (storeUsedIndex.contains(temp = random.nextInt(allTrajectoriesSize))) {

            }
            storeUsedIndex.add(temp);
            subSampleTrajectories.add(allTrajectories.get(temp));
        }
        return subSampleTrajectories;
    }

    /**
     * Compute the anomaly score of a trajectory.
     */
    private static double anomalyScore(int[] numOfIsolation, int subSampleSize) {
        double average;
        int sum = 0;
        for (int i : numOfIsolation) {
            sum += i;
        }
        average = sum * 1.0 / numOfIsolation.length;
        double cN = 2 * (Math.log(subSampleSize - 1) + 0.57721566) - 2.0 * (subSampleSize - 1) / subSampleSize;
        return Math.pow(2, -average / cN);
    }
}
