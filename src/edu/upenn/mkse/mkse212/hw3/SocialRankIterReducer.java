package edu.upenn.mkse.mkse212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
/*
 * Reducer sums the contribution from other nodes for source node, outputs source node with original values and new weight
 */
public class SocialRankIterReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		String orig = "";
		String edges = "";
		Double sum = 0.15;
		String test = "";
		//Iterate through value to sum contributions from other nodes and identify original node values
		for(Text val : value){
			String temp = val.toString();
			if(temp.contains("\t")){
				String[] vars = temp.split("\t");
				edges = vars[0];//edges
				orig = vars[1];//Original value
			}
			else{
				if(!temp.isEmpty())
					//test = test + "," + temp;
					sum += 0.85 * Double.parseDouble(temp); //Sum contributions from other nodes
			}
		}
		//Output key:Source node, value: edges + new weight of node + old weight of node
		context.write(key, new Text(edges + "\t" + sum.toString() + "\t" + orig));

	}
}
