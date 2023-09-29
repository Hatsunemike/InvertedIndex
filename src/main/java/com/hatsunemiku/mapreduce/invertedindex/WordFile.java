package com.hatsunemiku.mapreduce.invertedindex;

import com.sun.org.apache.xml.internal.security.algorithms.implementations.SignatureDSA;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordFile implements WritableComparable<WordFile> {
    String word;
    String filename;

    /**
     * 比较两个字符串的大小
     *
     * a小于b，返回-1；a==b，返回0；a>b，返回 1.
     * @param a
     * @param b
     * @return
     */
    private int strcmp(String a,String b) {
        if (a.length()!=b.length()) {
            return a.length() < b.length() ? -1 : 1;
        }
        int len = a.length();
        for(int i=0;i<len;++i) {
            if(a.charAt(i) != b.charAt(i))return a.charAt(i) < b.charAt(i) ? -1 : 1;
        }
        return 0;
    }

    /*===================== creator ======================*/
    public WordFile() {
        word = "";
        filename = "";
    }

    public WordFile(String val) {
        int index = val.indexOf(":");
        if (index == -1) {
            word = filename = "";
            return;
        }
        word = val.substring(0,index+1);
        filename = val.substring(index+1);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeUTF(filename);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word = in.readUTF();
        filename = in.readUTF();
    }

    @Override
    public String toString() {
        return word+":"+filename;
    }

    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof WordFile)) {
            return false;
        }
        WordFile now = (WordFile) obj;
        return now.getFilename() == this.filename && now.getWord() == this.word;
    }

    @Override
    public int compareTo(WordFile o) {
        int result = strcmp(this.word,o.getWord());
        if(result != 0)return result;
        result = strcmp(this.filename,o.filename);
        return result;
    }

    /** Compares two LongWritables. */

    /*===================== getter and setter ======================*/

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public String getFilename() {
        return filename;
    }

    public String getWord() {
        return word;
    }
}
