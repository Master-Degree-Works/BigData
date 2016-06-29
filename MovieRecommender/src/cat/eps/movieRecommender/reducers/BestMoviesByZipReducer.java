package cat.eps.movieRecommender.reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cat.eps.movieRecommender.writable.MovieWritable;

public class BestMoviesByZipReducer extends Reducer<Text,MovieWritable,NullWritable,MovieWritable>
{
	
	private TreeMap<Text,List<MovieWritable>> moviesByZip = new TreeMap<Text,List<MovieWritable>>();
	private TreeMap<MovieWritable,DoubleWritable> ratingsMovies = new TreeMap<MovieWritable,DoubleWritable>();
	
	
	public void reduce(Text key, Iterable<MovieWritable> values,Context context) throws IOException, InterruptedException
	{
		
		if(key.toString().startsWith("A")){
//			moviesByZip.put(new Text(key.toString().substring(1)),values);
		}

		long total = 0;
		MovieWritable movie = new MovieWritable();
		for (MovieWritable val : values) {
			total+=val.getOverallRating().get();
			movie = val;
		}
		
		movie.setOverallRating(new DoubleWritable(Double.valueOf(String.valueOf(total))));
				
//		context.write(NullWritable.get(),new Text(movie.toString()));  // Write reduce result {MovieWritable,ratingsCount}
	}
}
