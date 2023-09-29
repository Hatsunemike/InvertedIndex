package com.hatsunemiku.mapreduce.invertedindex;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

import java.io.IOException;

/**
 * KIN: Text, word
 * VIN: FileCount = {filename,count}
 * KEYOUT: ImmutableBytesWritable
 */
public class InvertedIndexReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> _values, Context context) throws IOException, InterruptedException {
        String word = key.toString();
        for(Text _val : _values) {
            FileCount val = new FileCount(_val.toString());
            String filename = val.getFilename();
            long count = val.getCount();
            Put put = new Put(Bytes.toBytes(word));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("filename"),Bytes.toBytes(filename));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("count"),Bytes.toBytes(count));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(word)),put);
        }
    }
}
