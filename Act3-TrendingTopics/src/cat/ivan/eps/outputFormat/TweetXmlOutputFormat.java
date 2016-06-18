package cat.ivan.eps.outputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cat.ivan.eps.writable.TweetWritable;

public class TweetXmlOutputFormat extends FileOutputFormat<Text,TweetWritable> {

	protected static class XMLRecordWriter extends RecordWriter<Text, TweetWritable> {
		private DataOutputStream out;

		public XMLRecordWriter(DataOutputStream out) throws IOException {
			this.out = out;
			out.writeBytes("<tweets>\n");
		}
		
		private void writeTag(String tag,String value) throws IOException{
			out.writeBytes("<"+tag+">"+value+"</"+tag+">");
		}
		
		public synchronized void write(Text key, TweetWritable value) throws IOException {
			out.writeBytes("<tweet>");
			this.writeTag("id", value.getTweetId().toString());
			this.writeTag("message", value.getMessage().toString());
			this.writeTag("lang", value.getIdioma().toString());
			this.writeTag("hashTag", value.getHashtag().toString());
			this.writeTag("sentiment", value.getSentiment().toString());
			out.writeBytes("</tweet>\n");
		} 

		@Override
		public void close(org.apache.hadoop.mapreduce.TaskAttemptContext context) throws IOException, InterruptedException {
			try { out.writeBytes("</tweets>\n");} 
			finally { out.close(); }
		}
	}
	
	
	public RecordWriter<Text, TweetWritable> getRecordWriter(TaskAttemptContext job) throws IOException {
		String extension = ".xml";
		  Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(job.getConfiguration());
		FSDataOutputStream fileOut = fs.create(file, false);
		return new XMLRecordWriter(fileOut);
	}


}