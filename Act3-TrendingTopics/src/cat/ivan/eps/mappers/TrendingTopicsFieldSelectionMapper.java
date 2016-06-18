package cat.ivan.eps.mappers;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cat.ivan.eps.writable.TweetWritable;

public class TrendingTopicsFieldSelectionMapper extends Mapper<Object, Text, Text,TweetWritable >{
	private String HASHTAG_PATTERN= "#(\\w+)";
	private Pattern pattern = Pattern.compile(HASHTAG_PATTERN);
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, TweetWritable>.Context context)
			throws IOException, InterruptedException {

		String lang = context.getConfiguration().get("lang");

		String[] lines = value.toString().split("\n");
		for (String line: lines)
		{
			String[] fields = line.toString().split("\t");
			String idField = fields[0];
			String textField = fields[1].split(",\"")[0];

			String langRegexp = "\"lang\":\"";
			int beginLangAttr = fields[1].lastIndexOf(langRegexp) + langRegexp.length();
			String langField = fields[1].substring(beginLangAttr, beginLangAttr+2);
			
			
			//Discard of non complete tweets
			if(textField.split("\":\"").length>1){
				String idFilteredField = idField.split(":")[1];
				String textFiltered = textField.split("\":\"").length>1?textField.split("\":\"")[1]:"";

				Matcher matcher = pattern.matcher(textField);
				String hashtag = "";
				while (matcher.find()) {
					hashtag = (hashtag.length()==0?hashtag:(" " + hashtag))+ matcher.group().trim();
				}

				//Afegim nomes els tweets amb hashtag i de l'idioma corresponent
				if((hashtag!=null && hashtag.length()>0) && (langField!=null && langField.equals(lang))){

					if(textFiltered.lastIndexOf("\"")==textFiltered.length()){
						textFiltered = textFiltered.substring(0, textFiltered.length());
					}
				
					TweetWritable tweetWritable = new TweetWritable(idFilteredField, textFiltered.toLowerCase(), langField);
					tweetWritable.setHashtag(hashtag!=null && hashtag.length()>1?new Text(hashtag.toLowerCase().trim()):new Text(""));

					context.write(new Text(idFilteredField), tweetWritable);
				}
			}
		}

	}
}
