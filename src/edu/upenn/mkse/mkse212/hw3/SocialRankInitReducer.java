package edu.upenn.mkse.mkse212.hw3;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/*
 * Outputs Node as key and a string with a list of its neighbors and a standard weight
 * 1 for all nodes (separated by a tab) as the value.
 */
public class SocialRankInitReducer extends Reducer<LongWritable, LongWritable, Text, Text> {

	public void reduce(LongWritable key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
		Long k = key.get();
		StringBuffer sb = new StringBuffer();
		for(LongWritable v : value){
			long temp = v.get();
			sb.append(temp + ",");
		}
		context.write(new Text(k.toString()), new Text(sb.toString().substring(0, sb.length() - 1) + "\t1"));
	}
}
