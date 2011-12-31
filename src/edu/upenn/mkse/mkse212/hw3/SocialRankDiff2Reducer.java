package edu.upenn.mkse.mkse212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
/*
 * Finds the max value by comparing all nodes in value and keeping track of max value.
 */
public class SocialRankDiff2Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		Double max = Double.MIN_VALUE;
		for(Text val : value){
			double temp = Double.parseDouble(val.toString());
			if(temp > max)
				max = temp;
		}
		context.write(new Text("Max Value"), new Text(max.toString()));
	}
}
