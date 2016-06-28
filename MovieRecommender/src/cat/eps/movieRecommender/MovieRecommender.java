package cat.eps.movieRecommender;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cat.eps.movieRecommender.jobControl.JobRunner;
import cat.eps.movieRecommender.mappers.BestRatedMoviesMapper;
import cat.eps.movieRecommender.mappers.InputReaderMapper;
import cat.eps.movieRecommender.mappers.MoviesMapper;
import cat.eps.movieRecommender.outputFormat.MovieOutputFormat;
import cat.eps.movieRecommender.reducers.MoviesReducer;
import cat.eps.movieRecommender.writable.MovieWritable;

public class MovieRecommender extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (args.length != 3) {
			System.err.printf("Usage: %s (<input dir> <output dir> <N Best Rated Movies>\n",
					getClass().getSimpleName());
			return -1;
		}

		/*******************JOB 1: Reader*******************/
		Job job1InputReader = Job.getInstance(conf);
		job1InputReader.setJarByClass(MovieRecommender.class);
		job1InputReader.setJobName("1.- Input Reader");
		job1InputReader.setMapperClass(InputReaderMapper.class);
		job1InputReader.setInputFormatClass(TextInputFormat.class);
		job1InputReader.setOutputFormatClass(TextOutputFormat.class);
		job1InputReader.setOutputKeyClass(LongWritable.class);
		job1InputReader.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1InputReader,new Path(args[0]));
		FileOutputFormat.setOutputPath(job1InputReader,new Path(args[1]+"/tmp/job1"));

		ControlledJob cJob1 = new ControlledJob(conf);
		cJob1.setJob(job1InputReader);


		/*******************JOB 2:Movies Mapper********************/
		Job job2BestRatedMovies = Job.getInstance(conf);
		job2BestRatedMovies.setJarByClass(MovieRecommender.class);
		job2BestRatedMovies.setJobName("2.- Movies Mapper");
		job2BestRatedMovies.setMapperClass(MoviesMapper.class);
		job2BestRatedMovies.setReducerClass(MoviesReducer.class);
		job2BestRatedMovies.setCombinerClass(MoviesReducer.class);
		job2BestRatedMovies.setInputFormatClass(TextInputFormat.class);
		job2BestRatedMovies.setOutputFormatClass(TextOutputFormat.class);
		job2BestRatedMovies.setOutputKeyClass(LongWritable.class);
		job2BestRatedMovies.setOutputValueClass(Text.class);
		//job2BestRatedMovies.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job2BestRatedMovies,new Path(args[1]+"/tmp/job1/part*"));
		FileOutputFormat.setOutputPath(job2BestRatedMovies,new Path(args[1]+"/tmp/job2"));

		ControlledJob cJob2 = new ControlledJob(conf);
		cJob2.setJob(job2BestRatedMovies);
		
		
		
		/*******************JOB 3:TOP N Movies********************/
		conf.set("NTop", args[2]);
		Job jo3BestRatedMovies = Job.getInstance(conf);
		jo3BestRatedMovies.setJarByClass(MovieRecommender.class);
		jo3BestRatedMovies.setJobName("3.- Top N Movies Mapper");
		jo3BestRatedMovies.setMapperClass(BestRatedMoviesMapper.class);
		//TODO: Implement inputFormat Class!!! jo3BestRatedMovies.setInputFormatClass(TextInputFormat.class);
		jo3BestRatedMovies.setInputFormatClass(TextInputFormat.class);
		jo3BestRatedMovies.setOutputFormatClass(MovieOutputFormat.class);
		jo3BestRatedMovies.setOutputKeyClass(NullWritable.class);
		jo3BestRatedMovies.setOutputValueClass(MovieWritable.class);
		
		FileInputFormat.addInputPath(jo3BestRatedMovies,new Path(args[1]+"/tmp/job2/part*"));
		FileOutputFormat.setOutputPath(jo3BestRatedMovies,new Path(args[1]+"/tmp/job3"));

		ControlledJob cJob3 = new ControlledJob(conf);
		cJob3.setJob(jo3BestRatedMovies);
		
		/*******************JOB 3: M Most active users********************/
		Job jo4MostActiveUsers = Job.getInstance(conf);
		
		
		JobControl jobctrl = new JobControl("jobctrl");
		jobctrl.addJob(cJob1);
		jobctrl.addJob(cJob2);
		jobctrl.addJob(cJob3);
		
		cJob2.addDependingJob(cJob1);
		cJob3.addDependingJob(cJob2);
		
		
		Thread jobRunnerThread = new Thread(new JobRunner(jobctrl));
		jobRunnerThread.start();
		while (!jobctrl.allFinished()) {
			System.out.println("Still running...");
			for(ControlledJob runningJob : jobctrl.getRunningJobList()){
				System.out.println("Running Job:" + runningJob.getJobName() +":"+runningJob.getJobState());
			}
			Thread.sleep(5000);
		}

		jobctrl.stop();
		System.out.println("Jobs Finished");
		//		System.out.println("Cleaning intermediate files...");
		//		FileSystem fs = FileSystem.get(conf);
		//		fs.delete(new Path(args[1]+"/tmp"), true);
		//		fs.close();
		System.out.println("done");

		return 0;
	}

	public static void main(String[] args)  {
		int exitCode;
		try {
			exitCode = ToolRunner.run(new MovieRecommender(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}