package cat.eps.movieRecommender.outputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cat.eps.movieRecommender.writable.JsonWritable;


public class JsonOutputFormat extends FileOutputFormat<LongWritable,JsonWritable> {

	protected static class JsonRecordWriter extends RecordWriter<LongWritable, JsonWritable> {
		private DataOutputStream out;

		public JsonRecordWriter(DataOutputStream out) throws IOException {
			this.out = out;
			out.writeBytes("{\"ratings\":[\n");
		}
		
		private void writeRecord(String name,String value) throws IOException{
			out.writeBytes("\""+name+"\":"+value);
		}
		
		public synchronized void write(LongWritable key, JsonWritable value) throws IOException {
			out.writeChars("{\"");
			this.writeRecord("id", key.toString()+",");
			this.writeRecord("rating", value.getRating().toString()+",");
			this.writeRecord("timestamp", value.getTimestamp().toString());
			//TODO: Add other fields
			out.writeChars("},");
		} 

		@Override
		public void close(org.apache.hadoop.mapreduce.TaskAttemptContext context) throws IOException, InterruptedException {
			try { out.writeBytes("\n]}");} 
			finally { out.close(); }
		}
	}
	
	
	public RecordWriter<LongWritable, JsonWritable> getRecordWriter(TaskAttemptContext job) throws IOException {
		String extension = ".json";
		  Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(job.getConfiguration());
		FSDataOutputStream fileOut = fs.create(file, false);
		return new JsonRecordWriter(fileOut);
	}


}
