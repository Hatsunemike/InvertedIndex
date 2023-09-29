package com.hatsunemiku.mapreduce.invertedindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 把Mapper的1加在一起
 * KIN: WordFile = {word,filename}
 * VIN: LongWritable, 1
 * KOUT: Text, 单词
 * VOUT: FileCount = {filename,counts}
 */
public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
    Text kOut = new Text();
    FileCount vOut = new FileCount();
    @Override
    protected void reduce(Text _key, Iterable<Text> _values, Context context) throws IOException, InterruptedException {
        WordFile key = new WordFile(_key.toString());
        String word = key.getWord();
        String filename = key.getFilename();
        long sum=0;
        for (Text _val : _values) {
            long val = Integer.valueOf(_val.toString()).longValue();
            sum += val;
        }
        kOut.set(word);
        vOut.setCount(sum);
        vOut.setFilename(filename);
        context.write(kOut,new Text(vOut.toString()));
    }
}
