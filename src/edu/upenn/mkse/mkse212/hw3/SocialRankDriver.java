package edu.upenn.mkse.mkse212.hw3;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.DoubleWritable.Comparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SocialRankDriver {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		//Creates job 
		Job job = new Job();
		job.setJarByClass(SocialRankDriver.class);
		if(args[0].equals("init")){

			//Sets the Input and output paths for files
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));

			//Sets Mapper and reducer classes
			job.setMapperClass(SocialRankInitMapper.class);
			job.setReducerClass(SocialRankInitReducer.class);

			//Sets Map Key and Value classes
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(LongWritable.class);
			job.setNumReduceTasks(Integer.parseInt(args[3]));
			
		}
		else if(args[0].equals("iter")){

			//Sets the Input and output paths for files
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));

			//Sets Mapper and reducer classes
			job.setMapperClass(SocialRankIterMapper.class);
			job.setReducerClass(SocialRankIterReducer.class);

			//Sets Map Key and Value classes
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setNumReduceTasks(Integer.parseInt(args[3]));

		}
		else if(args[0].equals("diff1")){

			//Sets the Input and output paths for files
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			job.setNumReduceTasks(Integer.parseInt(args[3]));

			//Sets Mapper and reducer classes
			job.setMapperClass(SocialRankDiff1Mapper.class);
			job.setReducerClass(SocialRankDiff1Reducer.class);

			//Sets Map Key and Value classes
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

		}
		else if(args[0].equals("diff2")){

			//Sets the Input and output paths for files
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));

			//Sets Mapper and reducer classes
			job.setMapperClass(SocialRankDiff2Mapper.class);
			job.setReducerClass(SocialRankDiff2Reducer.class);

			//Sets Map Key and Value classes
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

		}
		else if(args[0].equals("finish")){

			//Sets the Input and output paths for files
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			job.setNumReduceTasks(Integer.parseInt(args[3]));

			//Sets Mapper and reducer classes
			//job.setSortComparatorClass(ReduceComparator.class);
			job.setMapperClass(SocialRankFinishMapper.class);
			job.setReducerClass(SocialRankFinishReducer.class);

			//Sets Map Key and Value classes
			job.setMapOutputKeyClass(DoubleWritable.class);
			job.setMapOutputValueClass(Text.class);

		}
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
	public class ReduceComparator extends WritableComparator{

		protected ReduceComparator(Class<? extends WritableComparable> keyClass) {
			super(keyClass);
		}

		public int compare(WritableComparable a, WritableComparable b){
			int result = 0;
			Double x = Double.parseDouble(a.toString());
			Double y = Double.parseDouble(b.toString());
			if(x > y)
				result = 1;
			if(x < y)
				result = -1;
			return result;
		}

	}
}


