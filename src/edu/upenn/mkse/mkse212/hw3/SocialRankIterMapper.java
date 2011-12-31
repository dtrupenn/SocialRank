package edu.upenn.mkse.mkse212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
/*
 * Emits the contribution for each neighboring edge with the neighboring node and the source node with
 * it's weight and neighbors.
 */
public class SocialRankIterMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] vals = value.toString().split("\t");

		String source = vals[0]; //Source node
		if(!vals[1].equals("")){
			String[] edges = vals[1].split(","); //Array of neighbors
			int numEdge = edges.length; //Number of neighbors

			Double weight = Double.parseDouble(vals[2]); //Current weight

			Double cont = weight/numEdge; //Contribution for each edge

			for(String e : edges)
				context.write(new Text(e), new Text(cont.toString())); //Emits key: Neighbor Node, value: contribution

			context.write(new Text(source), new Text(vals[1] + "\t" + vals[2]));//Emits key: source node, value: neighboring nodes + weight
		}
		else
			context.write(new Text(vals[0]), new Text("\t" + vals[2]));
	}
}
