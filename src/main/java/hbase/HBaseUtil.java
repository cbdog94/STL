package hbase;

import constant.HBaseConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by cbdog94 on 17-3-17.
 */
public class HBaseUtil {

    private static Connection connection;

    HBaseUtil() {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", HBaseConstant.ZOOKEEPER_HOST);
            conf.set("hbase.zookeeper.property.clientPort", HBaseConstant.ZOOKEEPER_PORT);
//            conf.setInt("hbase.rpc.timeout", 600000);
//            conf.setInt("hbase.ipc.client.call.purge.timeout", 600000);
//            conf.setInt("hbase.hregion.memstore.block.multiplier", 8);
//            conf.setInt("hbase.client.operation.timeout", 600000);
//            conf.setInt("hbase.client.scanner.timeout.period", 600000);
//            conf.set("hbase.client.ipc.pool.type", "RoundRobinPool");
//            conf.setInt("hbase.client.ipc.pool.size", 10);
            connection = ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    String getColumnFromHBase(String tableName, byte[] rowKey, byte[] columnFamily, byte[] column) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Result resultCell = table.get(new Get(rowKey));
            return Bytes.toString(resultCell.getValue(columnFamily, column));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    Map<String, String> getBatchColumnFromHBase(String tableName, List<byte[]> rowKeys, byte[] columnFamily, byte[] column) {
        Map<String, String> map = new HashMap<>();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            List<Get> getList = rowKeys.stream().map(Get::new).collect(Collectors.toList());
            Result[] resultCells = table.get(getList);
            for (Result resultCell : resultCells) {
                map.put(Bytes.toString(resultCell.getRow()), Bytes.toString(resultCell.getValue(columnFamily, column)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    Map<String, String> getColumnFamilyFromHBase(String tableName, byte[] rowKey, byte[] columnFamily) {
        Map<String, String> resultMap = new HashMap<>();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Result resultCell = table.get(new Get(rowKey).addFamily(columnFamily));
            while (resultCell.advance()) {
                String key = Bytes.toString(CellUtil.cloneQualifier(resultCell.current()));
                String value = Bytes.toString(CellUtil.cloneValue(resultCell.current()));
                resultMap.put(key, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultMap;
    }

    public static void close() {
        try {
            if (connection != null)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
