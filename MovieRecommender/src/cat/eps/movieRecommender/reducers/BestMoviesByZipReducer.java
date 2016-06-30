package cat.eps.movieRecommender.reducers;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONException;

import cat.eps.movieRecommender.writable.MovieWritable;

public class BestMoviesByZipReducer extends Reducer<Text,Text,Text,Text>
{
//The key is the zipCode, the values are the movies rated by a user of this zipcode
	private TreeMap<Text,Iterable<Text>> moviesByZip = new TreeMap<Text,Iterable<Text>>();
	
	//The key is the movieId, the values is the movie in json with its overallRating and number of ocurrences
	private TreeMap<LongWritable,Text> ratingsMovies = new TreeMap<LongWritable,Text>();


	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
	{

		if(key.toString().startsWith("A")){
			moviesByZip.put(new Text(key.toString().substring(1)),values);
		}else{
			for(Text movieText : values){
				ratingsMovies.put(new LongWritable(Long.parseLong(key.toString())), movieText);
			}
		}
	}


	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		TreeMap<Text,MovieWritable> highestRatedByZipCode = new TreeMap<Text,MovieWritable>();

		for(Text userZip:moviesByZip.keySet()){

			MovieWritable highestRatedMovie = new MovieWritable();
			try {
				highestRatedMovie =  new MovieWritable(moviesByZip.get(userZip).iterator().next());
				
				for(Text movieText: moviesByZip.get(userZip)){
					try {
						MovieWritable movieWritable = new MovieWritable(movieText);

						MovieWritable movieWithRating = new MovieWritable(ratingsMovies.get(movieWritable.getMovieId()));

						movieWritable.setOverallRating(movieWithRating.getOverallRating());
						
						if(highestRatedMovie.getOverallRating().get()<movieWritable.getOverallRating().get()){
							highestRatedMovie = movieWritable;
						}
					} catch (JSONException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					highestRatedByZipCode.put(userZip, highestRatedMovie);
					context.write(userZip, new Text(highestRatedMovie.toString()));
			}
				
			} catch (JSONException e1) {
				e1.printStackTrace();
			}

		}

		
		
		
		super.cleanup(context);
	}


}
