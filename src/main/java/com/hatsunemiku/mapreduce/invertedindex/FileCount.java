package com.hatsunemiku.mapreduce.invertedindex;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FileCount implements Writable {
    private String filename;
    private long count;
    public FileCount() {
        super();
        filename = "";
        count = 0;
    }

    public FileCount(String s) {
        super();
        int index = s.indexOf(":");
        if(index == -1){
            filename = "";
            count = 0;
            return;
        }
        filename = s.substring(0,index+1);
        count = Integer.valueOf(s.substring(index+1)).longValue();
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(filename);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        filename = in.readUTF();
        count = in.readLong();
    }

    @Override
    public String toString() {
        return filename+":"+String.valueOf(count);
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public long getCount() {
        return count;
    }

    public String getFilename() {
        return filename;
    }
}