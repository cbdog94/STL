package mapreduce;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import constant.HBaseConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import util.CommonUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * build inverted index by the trajectory set.
 *
 * @author Bin Cheng
 */
public class CreateInvertIndex {

    @Parameter(names = {"--city", "-c"}, description = "Which city to be counted (SH/SZ/CD).", required = true, validateWith = CommonUtil.CityValidator.class)
    private String city;

    public static void main(String[] args) throws Exception {
        CreateInvertIndex main = new CreateInvertIndex();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(args);
        main.run();
    }

    private void run() throws Exception {

        Configuration config = HBaseConfiguration.create();
        config.setLong("mapred.task.timeout", 60 * 60 * 100);
        Job job = Job.getInstance(config, "CreateInvertIndex");
        job.setJarByClass(CreateInvertIndex.class);

        Scan scan = new Scan();
        scan.setCaching(1000);
        scan.setCacheBlocks(false);

        String trajectoryTable = CommonUtil.getTrajectoryTable(city);
        String invertedTable = CommonUtil.getInvertedTable(city);

        TableMapReduceUtil.initTableMapperJob(trajectoryTable, scan, Map.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(invertedTable, Reduce.class, job);

        job.setNumReduceTasks(48);

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }


    /**
     * put the inverted index into a HBase table
     * that the row key is the tile point,
     * the column is the trajectoryID and
     * the value is the index of title in the trajectory above.
     */
    public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val : values) {
                //value: 0006d72c38bb4c7ab035f6ebd8de6ad6,1
                //so the splits[0] is trajectoryID and the splits[1] is the index where one certain tile located in this trajectory
                String[] splits = val.toString().split(",");
                Put put = new Put(key.getBytes());
                put.addColumn(HBaseConstant.COLUMN_FAMILY_INDEX.getBytes(StandardCharsets.UTF_8), splits[0].getBytes(), splits[1].getBytes());
                context.write(null, put);
            }

        }
    }

    /**
     * split the whole trajectory into piece of tiles.
     */
    public static class Map extends TableMapper<Text, Text> {

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            // process data for the row from the Result instance.
            String trajectoryId = Bytes.toString(value.getRow());
            String trajectory = Bytes.toString(value.getValue(HBaseConstant.COLUMN_FAMILY_TRAJECTORY.getBytes(StandardCharsets.UTF_8), HBaseConstant.COLUMN_CELL.getBytes(StandardCharsets.UTF_8)));

            String[] tiles = trajectory.substring(1, trajectory.length() - 1).split(", ");
            for (int i = 0; i < tiles.length; i++) {
                context.write(new Text(tiles[i]), new Text(trajectoryId + "," + i));
            }
        }

    }
}
