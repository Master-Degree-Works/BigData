package cat.eps.movieRecommender.mappers;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cat.eps.movieRecommender.writable.MovieWritable;

public class BestRatedMoviesMapper  extends Mapper<LongWritable, Text,NullWritable,MovieWritable >
{

	private TreeMap<DoubleWritable,MovieWritable> topMovies = new TreeMap<DoubleWritable,MovieWritable>();

	public void map(LongWritable key,Text  value, Context context) throws IOException, InterruptedException {

		String[] goodValues = value.toString().split("\t");
		
		MovieWritable movie = new MovieWritable(goodValues[1],true);

		topMovies.put(movie.getOverallRating(),movie);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		String nTop = context.getConfiguration().get("NTop");
		Integer nTopInt = new Integer(nTop);
		
		boolean allItems = (nTopInt ==999);
		NavigableSet<DoubleWritable> nSet = topMovies.descendingKeySet();
		  
		int itemCounter=0;
		
		for (DoubleWritable key: nSet) {
			MovieWritable movie = topMovies.get(key);
			if(itemCounter>=nTopInt && !allItems){
				break;
			}

			context.write(NullWritable.get(),movie);
			itemCounter++;
		}
	}
}
