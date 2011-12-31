package edu.upenn.mkse.mkse212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/*
 * Emits each node and the values that it connects to.
 */
public class SocialRankInitMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String k = value.toString();
		String[] vals = k.split("\t");
		long a = Long.parseLong(vals[0]);
		long b = Long.parseLong(vals[1]);
		context.write(new LongWritable(a), new LongWritable(b));
	}
}
