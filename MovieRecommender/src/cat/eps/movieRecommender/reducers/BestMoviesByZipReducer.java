package cat.eps.movieRecommender.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONException;

import cat.eps.movieRecommender.writable.MovieWritable;

public class BestMoviesByZipReducer extends Reducer<Text,Text,Text,Text>
{
//The key is the zipCode, the values are the movies rated by a user of this zipcode
	private TreeMap<Text,List<Text>> moviesByZip = new TreeMap<Text,List<Text>>();
	
	//The key is the movieId, the values is the movie in json with its overallRating and number of ocurrences
	private TreeMap<LongWritable,Text> ratingsMovies = new TreeMap<LongWritable,Text>();


	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		
		for(Text iterable:values){
			if(key.toString().contains("AAAA")){
				String userZip = key.toString().replace("AAAA","");
				String movie = iterable.toString();
				if(moviesByZip.containsKey(userZip)){
					moviesByZip.get(userZip).add(new Text(movie));
				}else{
					List<Text> movies = new ArrayList<Text>();
					movies.add(new Text(movie));
					moviesByZip.put(new Text(userZip),movies);
				}
			}
			else{
				ratingsMovies.put(new LongWritable(Long.parseLong(key.toString())), iterable);
			}
		}		
	}


	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		TreeMap<Text,MovieWritable> highestRatedByZipCode = new TreeMap<Text,MovieWritable>();
		context.write(new Text("Map keyset Size"), new Text(String.valueOf(moviesByZip.keySet().size())));
		for(Text userZip:moviesByZip.keySet()){

			MovieWritable highestRatedMovie = null;
			context.write(userZip, new Text(""));

				for(Text movieText: moviesByZip.get(userZip)){
					try {
						
						MovieWritable movieWritable = new MovieWritable(movieText);

						MovieWritable movieWithRating = new MovieWritable(ratingsMovies.get(movieWritable.getMovieId()));

						movieWritable.setOverallRating(movieWithRating.getOverallRating());
						
						if(highestRatedMovie==null){
							highestRatedMovie = movieWritable;					
						}
						
						if(highestRatedMovie.getOverallRating().get()<movieWritable.getOverallRating().get()){
							highestRatedMovie = movieWritable;
						}
						context.write(userZip, new Text(highestRatedMovie.toString()));
					} catch (JSONException e) {
						e.printStackTrace();
						context.write(new Text("Exception!!!!"), new Text(e.getMessage()));
						throw new IOException(e.getCause());
					}
//					highestRatedByZipCode.put(userZip, highestRatedMovie);
			}
		}
	}


}
