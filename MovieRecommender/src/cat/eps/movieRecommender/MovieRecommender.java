package cat.eps.movieRecommender;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cat.eps.movieRecommender.jobControl.JobRunner;
import cat.eps.movieRecommender.mappers.InputReaderMapper;


public class MovieRecommender extends Configured implements Tool
{


	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (args.length != 2) {
			System.err.printf("Usage: %s (<input file> <output dir>\n",
					getClass().getSimpleName());
			return -1;
		}


		
		/******************************* JOB 1*/
		Job job1InputReader = Job.getInstance(conf);
		job1InputReader.setJarByClass(MovieRecommender.class);
		job1InputReader.setJobName("1.- Input Reader");
		job1InputReader.setMapperClass(InputReaderMapper.class);
		job1InputReader.setInputFormatClass(TextInputFormat.class);
		job1InputReader.setOutputFormatClass(TextOutputFormat.class);
		job1InputReader.setOutputKeyClass(Text.class);
		job1InputReader.setOutputValueClass(Text.class);

		String fileName = "";

		FileInputFormat.addInputPath(job1InputReader,new Path(args[0]+fileName));
		FileOutputFormat.setOutputPath(job1InputReader,new Path(args[1]+"/tmp/job1"));

		ControlledJob cJob1 = new ControlledJob(conf);
		cJob1.setJob(job1InputReader);
		
		
		
	
		JobControl jobctrl = new JobControl("jobctrl");
		jobctrl.addJob(cJob1);
	
		
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

