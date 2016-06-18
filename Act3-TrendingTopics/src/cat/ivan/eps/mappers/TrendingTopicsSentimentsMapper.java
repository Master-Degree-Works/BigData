package cat.ivan.eps.mappers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import cat.ivan.eps.writable.TweetWritable;

/**
 * MAPPER
 */
public class TrendingTopicsSentimentsMapper extends Mapper<LongWritable, Text, Text, TweetWritable>
{

	private Set<String> positiveWords = new HashSet<>();
	private Set<String> negativeWords = new HashSet<>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		try{
			URI[]  cachedFiles = context.getCacheFiles();
			if(cachedFiles != null && cachedFiles.length > 0) {
				readPositiveFile(cachedFiles[0].getPath().substring(cachedFiles[0].getPath().lastIndexOf('/') + 1));
				readNegativeFile(cachedFiles[1].getPath().substring(cachedFiles[1].getPath().lastIndexOf('/') + 1));
			}
		} catch(IOException ex) {
			System.err.println("Exception in mapper setup: " + ex.getMessage());
		}
	}



	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


		String tweetXmlString = value.toString();  

		TweetWritable tweet =  buildTweetWritable(tweetXmlString);

		double positiveIndex = 0;
		double negativeIndex = 0;
		double tweetMsgLenght = 0;

		//For counting the lenght and the words, we must escape the special encoding like emojis...
		String tweetMsg = StringEscapeUtils.unescapeJava(tweet.getMessage().toString().replaceAll("<!\\[CDATA\\[","").replaceAll("\\]\\]>",""));

		tweetMsgLenght = tweetMsg.length();

		String[] tweetMsgSplitted = tweetMsg.split("\\s+");

		for(String word : tweetMsgSplitted){
			if(positiveWords.contains(word.toLowerCase())){
				positiveIndex++;
			}
			else if(negativeWords.contains(word.toLowerCase())){
				negativeIndex++;
			}
		}

		double sentiment = (positiveIndex-negativeIndex)/tweetMsgLenght;

		tweet.setSentiment(new DoubleWritable(sentiment));

		context.write(new Text(),tweet);
	}



	private void readPositiveFile(String filePath) {
		try{
			System.out.println("[readPositiveFile] READING FILE --> " + filePath);
			BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
			String positiveWord = null;
			while((positiveWord = bufferedReader.readLine()) != null) {
				positiveWords.add(positiveWord.toLowerCase());
			}
			bufferedReader.close();
		} catch(IOException ex) {
			System.err.println("[readPositiveFile] Exception while reading stop words file: " + ex.getMessage());
		}
	}

	private void readNegativeFile(String filePath) {
		try{
			System.out.println("[readNegativeFile] READING FILE --> " + filePath);
			BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
			String negativeWord = null;
			while((negativeWord = bufferedReader.readLine()) != null) {
				negativeWords.add(negativeWord.toLowerCase());
			}
			bufferedReader.close();
		} catch(IOException ex) {
			System.err.println("[readNegativeFile] Exception while reading stop words file: " + ex.getMessage());
		}
	}

	public static TweetWritable buildTweetWritable(String tweetXmlString) {

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();  
		DocumentBuilder builder;  
		TweetWritable tweet = new TweetWritable();
		try  
		{  
			builder = factory.newDocumentBuilder();  
			Document document = builder.parse(new InputSource(new StringReader(tweetXmlString)));  
			Element rootElement = document.getDocumentElement();

			NodeList nodeListIds = rootElement.getElementsByTagName("id");
			if (nodeListIds != null && nodeListIds.getLength() > 0) {
				NodeList subList = nodeListIds.item(0).getChildNodes();

				if (subList != null && subList.getLength() > 0) {
					tweet.setTweetId(new Text(subList.item(0).getTextContent()));
				}
			}

			NodeList nodeListMessages = rootElement.getElementsByTagName("message");
			if (nodeListMessages != null && nodeListMessages.getLength() > 0) {
				NodeList subList = nodeListMessages.item(0).getChildNodes();

				if (subList != null && subList.getLength() > 0) {
					tweet.setMessage(new Text("<![CDATA["+subList.item(0).getNodeValue().replaceAll("\"","").replaceAll("\n","").replaceAll("[^\\x20-\\xFF]","")+"]]>"));
				}
			}

			NodeList nodeListLangs = rootElement.getElementsByTagName("lang");
			if (nodeListLangs != null && nodeListLangs.getLength() > 0) {
				NodeList subList = nodeListLangs.item(0).getChildNodes();

				if (subList != null && subList.getLength() > 0) {
					tweet.setIdioma(new Text(subList.item(0).getTextContent()));
				}
			}

			NodeList nodeListHastags = rootElement.getElementsByTagName("hashTag");
			if (nodeListHastags != null && nodeListHastags.getLength() > 0) {
				NodeList subList = nodeListHastags.item(0).getChildNodes();

				if (subList != null && subList.getLength() > 0) {
					String HASHTAG_PATT = "(?:^|\\s|[\\p{Punct}&&[^/]])(#[\\p{L}0-9-_]+)";
					Pattern patt = Pattern.compile(HASHTAG_PATT);
					Matcher matc = patt.matcher(tweet.getMessage().toString());
					String hashtag = "";
					boolean isFirst = true;
					while (matc.find()) {
						if(isFirst){
							hashtag = matc.group().trim();
							isFirst = false;
						}else{
							hashtag += " " + (matc.group().trim()); 
						}
					}
					tweet.setHashtag(new Text(hashtag.replaceAll("\\[","")));
				}
			}


		} catch (Exception e) {  
			e.printStackTrace();  
		} 
		return tweet;
	}
}
