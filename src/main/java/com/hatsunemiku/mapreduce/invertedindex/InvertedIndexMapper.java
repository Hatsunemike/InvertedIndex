package com.hatsunemiku.mapreduce.invertedindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * KIN：LongWritable，行偏移量
 * VIN：Text，该行内容（ID 只有空格和拉丁字母的文本）
 * KOUT：WordFile = {word,filename}
 * VOUT：LongWritable, 1
 */
public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    FileSplit split = null;
    WordFile kOut = new WordFile();
    LongWritable vOut = new LongWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String sentence = value.toString();
        String[] words = sentence.split(" ");
        split = (FileSplit)context.getInputSplit();
        String filename = split.getPath().getName();
        for (int i=1;i<words.length;++i) { // 从1开始是因为第一个数是行号
            String word = words[i];
            if(word.length() == 0)continue;
            kOut.setWord(word);kOut.setFilename(filename);
            context.write(new Text(kOut.toString()), new Text(vOut.toString()));
        }
    }
}