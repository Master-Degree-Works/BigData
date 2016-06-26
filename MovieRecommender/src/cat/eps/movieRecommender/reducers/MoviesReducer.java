package cat.eps.movieRecommender.reducers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cat.eps.movieRecommender.writable.MovieWritable;

public class MoviesReducer extends Reducer<LongWritable,Text,NullWritable,Text>
{
	public void reduce(LongWritable key, Iterable<MovieWritable> values,Context context) throws IOException, InterruptedException
	{
		long total = 0;
		MovieWritable movie = new MovieWritable();
		for (MovieWritable val : values) {
			total+=val.getOverallRating().get();
			movie = val;
		}
		
		movie.setOverallRating(new LongWritable(total));
		
		
		context.write(NullWritable.get(),new Text(movie.toString()));  // Write reduce result {MovieWritable,ratingsCount}
	}

}
