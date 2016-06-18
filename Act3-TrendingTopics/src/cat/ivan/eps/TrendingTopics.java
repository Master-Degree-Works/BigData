package cat.ivan.eps;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.fieldsel.FieldSelectionHelper;
import org.apache.hadoop.mapreduce.lib.fieldsel.FieldSelectionMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cat.ivan.eps.inputFormat.TweetXmlInputFormat;
import cat.ivan.eps.jobControl.JobRunner;
import cat.ivan.eps.mappers.ReadTopNMapper;
import cat.ivan.eps.mappers.ReadTweetsMapper;
import cat.ivan.eps.mappers.TrendingTopicsFieldSelectionMapper;
import cat.ivan.eps.mappers.TrendingTopicsSentimentsMapper;
import cat.ivan.eps.mappers.TrendingTopicsTopNMapper;
import cat.ivan.eps.outputFormat.TweetXmlOutputFormat;
import cat.ivan.eps.reducers.GathererReducer;
import cat.ivan.eps.reducers.TrendingTopicsFieldSelectionReducer;
import cat.ivan.eps.reducers.TrendingTopicsHashtagReducer;
import cat.ivan.eps.reducers.TrendingTopicsSentimentsReducer;
import cat.ivan.eps.writable.TweetWritable;


public class TrendingTopics extends Configured implements Tool
{


	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (args.length != 6) {
			System.err.printf("Usage: %s (<input tweets file> <output dir> <N> <language> <positive-words file> <negative-words file>\n",
					getClass().getSimpleName());
			return -1;
		}


		String lang = args[3];
	
		/******************************* JOB 1*/
		conf.set(RegexMapper.PATTERN, "#(\\w+)");
		Job job1TrendingTopics = Job.getInstance(conf);
		job1TrendingTopics.setJarByClass(TrendingTopics.class);
		job1TrendingTopics.setJobName("1.- Regexp Job");
		//We use regexpMapper
		job1TrendingTopics.setMapperClass(RegexMapper.class);
		job1TrendingTopics.setCombinerClass(TrendingTopicsHashtagReducer.class);
		job1TrendingTopics.setReducerClass(TrendingTopicsHashtagReducer.class);
		job1TrendingTopics.setOutputKeyClass(Text.class);
		job1TrendingTopics.setOutputValueClass(LongWritable.class);

		String fileName = "";
		
		if(lang!=null && lang.equals("es")){
			fileName = "/tweets_es.json";
		}else if (lang.equals("en")){
			fileName = "/tweets_en.json";
		}else{
			fileName ="/tweets.json";
		}
		
		FileInputFormat.addInputPath(job1TrendingTopics,new Path(args[0]+fileName));
		FileOutputFormat.setOutputPath(job1TrendingTopics,new Path(args[1]+"/tmp/job1"));

		ControlledJob cJob1 = new ControlledJob(conf);
		cJob1.setJob(job1TrendingTopics);
		
		

		/******************************** JOB 2*/
		conf.set("NTop", args[2]);

		Job job2TopNMappers = Job.getInstance(conf);
		job2TopNMappers.setJobName("2.- Top N Job");
		job2TopNMappers.setJarByClass(TrendingTopics.class);
		job2TopNMappers.setMapperClass(TrendingTopicsTopNMapper.class);
		job2TopNMappers.setOutputKeyClass(Text.class);
		job2TopNMappers.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job2TopNMappers, new Path(args[1]+"/tmp/job1/part*"));
		FileOutputFormat.setOutputPath(job2TopNMappers, new Path(args[1]+"/tmp/job2"));

		job2TopNMappers.setNumReduceTasks(3);
		
		ControlledJob cJob2 = new ControlledJob(conf);
		cJob2.setJob(job2TopNMappers);

		
		/******************************** JOB 4 : FieldSelection Mapper*/
		conf.set("lang",lang);
		conf.set(FieldSelectionHelper.DATA_FIELD_SEPERATOR,",");
		conf.set(FieldSelectionHelper.MAP_OUTPUT_KEY_VALUE_SPEC, "1:3,4-");
		conf.set(FieldSelectionHelper.REDUCE_OUTPUT_KEY_VALUE_SPEC,"1:3,4-");	
		Job job4FieldSelection = Job.getInstance(conf);
		job4FieldSelection.setJobName("4.- FieldSelection Job");
		job4FieldSelection.setJarByClass(TrendingTopics.class);
		job4FieldSelection.setMapperClass(FieldSelectionMapper.class);
		job4FieldSelection.setOutputKeyClass(Text.class);
		job4FieldSelection.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job4FieldSelection, new Path(args[0]+fileName));
		FileOutputFormat.setOutputPath(job4FieldSelection, new Path(args[1]+"/tmp/job4"));
		job4FieldSelection.setNumReduceTasks(1);

		
		ControlledJob cJob4 = new ControlledJob(conf);
		cJob4.setJob(job4FieldSelection);

		/******************************* JOB 4.1*/
		Job job41FieldSelection = Job.getInstance(conf);
		job41FieldSelection.setJobName("4.1.- FieldMapper Job");
		job41FieldSelection.setJarByClass(TrendingTopics.class);
		job41FieldSelection.setMapperClass(TrendingTopicsFieldSelectionMapper.class);
		job41FieldSelection.setReducerClass(TrendingTopicsFieldSelectionReducer.class);
		job41FieldSelection.setCombinerClass(TrendingTopicsFieldSelectionReducer.class);
		job41FieldSelection.setOutputKeyClass(Text.class);
		job41FieldSelection.setOutputValueClass(TweetWritable.class);
		job41FieldSelection.setOutputFormatClass(TweetXmlOutputFormat.class);
		FileInputFormat.addInputPath(job41FieldSelection, new Path(args[1]+"/tmp/job4/part*"));
		FileOutputFormat.setOutputPath(job41FieldSelection, new Path(args[1]+"/tmp/job4.1"));

		ControlledJob cJob41 = new ControlledJob(conf);
		cJob41.setJob(job41FieldSelection);

		/******************************* JOB 5*/
		JobConf jobConfReadTopN = new JobConf(getConf(), ReadTopNMapper.class);
		Job job5Gatherer = Job.getInstance(conf);
		job5Gatherer.setJobName("5.- Gatherer Job");
		job5Gatherer.setJarByClass(TrendingTopics.class);
		job5Gatherer.setMapperClass(ReadTopNMapper.class);
		job5Gatherer.setMapperClass(ReadTweetsMapper.class); 
		job5Gatherer.setReducerClass(GathererReducer.class);
		MultipleInputs.addInputPath(job5Gatherer, new Path(args[1]+"/tmp/job2/part*"), TextInputFormat.class,  ReadTopNMapper.class);
		MultipleInputs.addInputPath(job5Gatherer, new Path(args[1]+"/tmp/job4.1/part*"), TweetXmlInputFormat.class, ReadTweetsMapper.class);
		job5Gatherer.setOutputKeyClass(Text.class);
		job5Gatherer.setOutputValueClass(Text.class);
		job5Gatherer.setOutputFormatClass(TweetXmlOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job5Gatherer, new Path(args[1]+"/tmp/job5"));
		
		ControlledJob cJob5 = new ControlledJob(jobConfReadTopN);
		cJob5.setJob(job5Gatherer);
		

		/******************************** JOB 3*/
		Job job3Sentiments = Job.getInstance(conf);
		job3Sentiments.setJobName("3.- Sentiment Job");
		job3Sentiments.setJarByClass(TrendingTopics.class);
		job3Sentiments.setMapperClass(TrendingTopicsSentimentsMapper.class);
		job3Sentiments.setReducerClass(TrendingTopicsSentimentsReducer.class);
		job3Sentiments.setCombinerClass(TrendingTopicsSentimentsReducer.class);
		
		job3Sentiments.setInputFormatClass(TweetXmlInputFormat.class);
		job3Sentiments.setOutputKeyClass(Text.class);
		job3Sentiments.setOutputValueClass(TweetWritable.class);
		job3Sentiments.setOutputFormatClass(TweetXmlOutputFormat.class);

		job3Sentiments.addCacheFile(new Path(args[4]).toUri());
		job3Sentiments.addCacheFile(new Path(args[5]).toUri());
		FileInputFormat.addInputPath(job3Sentiments,new Path(args[1]+"/tmp/job5/part*"));
		FileOutputFormat.setOutputPath(job3Sentiments, new Path(args[1]+"/finalResult/"));

		ControlledJob cJob3 = new ControlledJob(conf);
		cJob3.setJob(job3Sentiments);

		// Show cache files.
		URI[] cacheFiles = job3Sentiments.getCacheFiles();
		if(cacheFiles != null) {
			for (URI cacheFile : cacheFiles) {
				System.out.println("Cache file ->" + cacheFile);
			}
		}

		
		
		//Order: job1,job2|job4,job41 => job5,job3
		JobControl jobctrl = new JobControl("jobctrl");
		jobctrl.addJob(cJob1);
		jobctrl.addJob(cJob2);
		jobctrl.addJob(cJob3);
		jobctrl.addJob(cJob4);
		jobctrl.addJob(cJob41);
		jobctrl.addJob(cJob5);
		
		cJob2.addDependingJob(cJob1);
		
		cJob3.addDependingJob(cJob5);
		
		cJob41.addDependingJob(cJob4);
		
		cJob5.addDependingJob(cJob2);
		cJob5.addDependingJob(cJob41);
		
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
		System.out.println("Cleaning intermediate files...");
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]+"/tmp"), true);
		fs.close();
		System.out.println("done");


		return 0;
	}


	public static void main(String[] args)  {
		int exitCode;
		try {
			exitCode = ToolRunner.run(new TrendingTopics(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

