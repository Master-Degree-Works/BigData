package cat.ivan.eps.mappers;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MAPPER TOP N
 */
public class TrendingTopicsTopNMapper extends Mapper<Object, Text, Text, LongWritable>
{

	private TreeMap<Long,String> topHashTags = new TreeMap<Long,String>();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//2.- Count the top N hashtags in topHashTags

		String[] lines = value.toString().split("\n") ;
		for (String line: lines)
		{
			String[] item = line.toString().split("\t") ;
			topHashTags.put(new Long(item[1].toString().trim()),item[0].toString().trim());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		String nTop = context.getConfiguration().get("NTop");
		Integer nTopInt = new Integer(nTop);
		
		boolean allItems = (nTopInt ==999);
		NavigableSet<Long> nSet = topHashTags.descendingKeySet();
		  
		int itemCounter=0;
		
		for (Long key: nSet) {
			String value = topHashTags.get(key);
			if(itemCounter>=nTopInt && !allItems){
				break;
			}
			context.write(new Text(value),new LongWritable(key));
			itemCounter++;
		}
	}
}
