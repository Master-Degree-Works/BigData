package cat.ivan.eps.partitioners;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TrendingTopicsPartitioner extends Partitioner<Text, LongWritable> {

	//TODO!!!!!!
	@Override
	public int getPartition(Text key, LongWritable value, int numPartitions) {
		final char TopFirstPartition='g', TopSecondPartition='p';

		String word = key.toString();
		char letter;
		if (!word.isEmpty())
			letter = word.toLowerCase().charAt(0);
		else return (0);
		if (numPartitions != 3) {
			return (int) (letter - 'a') % numPartitions;
		} else {
			if (letter <= TopFirstPartition)
				return (0);
			else if (letter > TopFirstPartition && letter <= TopSecondPartition)
				return (1);
			else if (letter > TopSecondPartition)
				return (2);
		}
		return(0);
	}
}
