import com.hatsunemiku.mapreduce.invertedindex.InvertedIndexDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class InvertedIndex {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf = HBaseConfiguration.create(conf);
        int result = ToolRunner.run(conf,new InvertedIndexDriver(),args);
        System.exit(result == 0 ? 1 : 0);
    }
}
