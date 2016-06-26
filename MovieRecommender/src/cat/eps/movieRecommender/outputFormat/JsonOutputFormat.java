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


public class JsonOutputFormat extends FileOutputFormat<JsonWritable,LongWritable> {

	protected static class JsonRecordWriter extends RecordWriter<JsonWritable,LongWritable > {
		private DataOutputStream out;

		public JsonRecordWriter(DataOutputStream out) throws IOException {
			this.out = out;
			out.writeBytes("{\"ratings\":[\n");
		}

		private void writeRecord(String name,String value) throws IOException{
			out.writeBytes("\""+name+"\":"+value);
		}

		public synchronized void write(JsonWritable key, LongWritable value) throws IOException {
			out.writeBytes("{");
			this.writeRecord("id", key.getId()+",");
			this.writeRecord("rating", key.getRating().toString()+",");
			this.writeRecord("timestamp", key.getTimestamp().toString());
			//TODO: Add other fields
			out.writeBytes("},\n");
		} 

		@Override
		public void close(org.apache.hadoop.mapreduce.TaskAttemptContext context) throws IOException, InterruptedException {
			try { out.writeBytes("\n]}");} 
			finally { out.close(); }
		}
	}


	public RecordWriter<JsonWritable,LongWritable > getRecordWriter(TaskAttemptContext job) throws IOException {
		String extension = ".json";
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(job.getConfiguration());
		FSDataOutputStream fileOut = fs.create(file, false);
		return new JsonRecordWriter(fileOut);
	}


}
