package edu.upenn.mkse.mkse212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
/*
 * Calculates the difference in weight and outputs the difference as the value
 * and the node as the key.
 */
public class SocialRankDiff1Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		for(Text val : value){
			String[] vals = val.toString().split("\t");
			Double diff = Math.abs(Double.parseDouble(vals[0]) - Double.parseDouble(vals[1]));
			context.write(key, new Text(diff.toString()));
		}
	}
}
