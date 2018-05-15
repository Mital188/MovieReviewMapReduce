package HDFS_assignment_2.movieReview.partii;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class Partii extends Configured implements Tool {
	

	public static void main( String[] args )throws Exception
	{		
		try {
			int res =ToolRunner.run(new Partii(),args);
			System.exit(res);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	public static class RatingMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{

		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			//Ignores the header row from the csv file
			if(key.get() == 0 && value.toString().contains("userId")) {
				return;
			}
			
			String line = value.toString();
			String[] ratings = line.split(",");
			
			int movieId = Integer.parseInt(ratings[1]);
			double rating = Double.parseDouble(ratings[2]);
			
			IntWritable movieKey = new IntWritable(movieId);
			DoubleWritable movieValue = new DoubleWritable(rating);
			
			context.write(movieKey, movieValue);
			
			
		}
	}

	public static class RatingReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
	    @Override
	    public void reduce(IntWritable movieKey, Iterable<DoubleWritable> ratings, Context context)
	            throws IOException, InterruptedException{

	        double averageRating = 0;
	        int count = 0;

	        // Add up all the ratings
	        for (DoubleWritable r: ratings) {
	            averageRating += r.get();
	            count++;
	        }

	        // Divide by the number of ratings to get the average for this movie
	        averageRating /= count;
	        DoubleWritable average = new DoubleWritable(averageRating);

	        // Output the average rating for this movie
	        context.write(movieKey, average);
	    }
	}

	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
		conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
		conf.set("mapreduce.framework.name", "yarn");
		Job job = Job.getInstance(conf, "movie review");
		job.setJarByClass(Partii.class);
		// Set Mapper and Reducer Class
		job.setMapperClass(RatingMapper.class);
		job.setReducerClass(RatingReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	

}
