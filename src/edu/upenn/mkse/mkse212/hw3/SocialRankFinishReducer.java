package edu.upenn.mkse.mkse212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
/*
 * Ouputs Node as key and Weight as the value
 */
public class SocialRankFinishReducer extends Reducer<DoubleWritable, Text, Text, Text> {

	public void reduce(DoubleWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		for(Text val : value){
			String[] vals = val.toString().split("\t");
			context.write(new Text(vals[0]), new Text(vals[1]));
		}	
	}
}
