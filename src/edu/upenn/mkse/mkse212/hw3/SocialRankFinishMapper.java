package edu.upenn.mkse.mkse212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
/*
 * Emits each node and the values that it connects to. Key: weight, Value: "Node\tWeight"
 */
public class SocialRankFinishMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[]  val = value.toString().split("\t");
		double weight = Double.parseDouble(val[3]);
		weight = 100000 - weight;
		context.write(new DoubleWritable(weight), new Text(val[0] + "\t" + val[3]));
	}
}
