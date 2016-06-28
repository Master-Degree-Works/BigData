package cat.eps.movieRecommender.outputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cat.eps.movieRecommender.writable.MovieWritable;


public class MovieOutputFormat extends FileOutputFormat<NullWritable,MovieWritable> {

	protected static class MovieRecordWriter extends RecordWriter<NullWritable,MovieWritable > {
		private DataOutputStream out;

		public MovieRecordWriter(DataOutputStream out) throws IOException {
			this.out = out;
			out.writeBytes("{\"movies\":[\n");
		}

		private void writeRecordString(String name,String value) throws IOException{
			out.writeBytes("\""+name+"\":"+value);
		}
		
		private void writeRecord(String name,String value) throws IOException{
			out.writeBytes("\""+name+"\":"+value);
		}
		
		public synchronized void write(NullWritable key, MovieWritable value) throws IOException {
			out.writeBytes("{");
			this.writeRecord("movieId", value.getMovieId()+",");
			this.writeRecordString("movieTitle","\""+ value.getMovieTitle().toString()+"\",");
			this.writeRecordString("movieGenre","\""+value.getMovieGenre().toString()+"\",");
			this.writeRecord("rating",value.getOverallRating().toString()+",");
			this.writeRecord("numberOfOcurrences",value.getNumberOfOcurrences().toString());
			out.writeBytes("},\n");
		} 

		@Override
		public void close(org.apache.hadoop.mapreduce.TaskAttemptContext context) throws IOException, InterruptedException {
			try { out.writeBytes("]}");} 
			finally { out.close(); }
		}
	}


	public RecordWriter<NullWritable,MovieWritable > getRecordWriter(TaskAttemptContext job) throws IOException {
		String extension = ".json";
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(job.getConfiguration());
		FSDataOutputStream fileOut = fs.create(file, false);
		return new MovieRecordWriter(fileOut);
	}


}
