package edu.upenn.mkse.mkse212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
/*
 * Emits values into  as key with current weight and new weight
 */
public class SocialRankDiff1Mapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] vals = value.toString().split("\\t");
		String enc = vals[3] + "\t" + vals[2]; //Gets the current weight and the new weight
		context.write(new Text(vals[0]), new Text(enc));
	}
}
