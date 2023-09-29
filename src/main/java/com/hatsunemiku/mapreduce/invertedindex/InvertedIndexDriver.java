package com.hatsunemiku.mapreduce.invertedindex;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

public class InvertedIndexDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(this.getConf());
        String inputPath = "/indexinput";
        if (args.length > 0) {
            inputPath = new String(args[0]);
        }
        job.setJarByClass(InvertedIndexDriver.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(inputPath));

        String tablename = "invindex";
        TableMapReduceUtil.initTableReducerJob(tablename, InvertedIndexReducer.class, job);

        return job.waitForCompletion(true) ? 1 : 0;
    }
}
