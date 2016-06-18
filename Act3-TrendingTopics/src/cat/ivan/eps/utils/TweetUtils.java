package cat.ivan.eps.utils;

import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.Text;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import cat.ivan.eps.writable.TweetWritable;

public class TweetUtils {
	
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
					tweet.setMessage(new Text("<![CDATA["+subList.item(0).getNodeValue()+"]]>"));
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
	
	public static String tweetWritableToString(TweetWritable tweetWritable) {
	
		String output = "<tweet>";
		output+="<tweetId>"+tweetWritable.getTweetId()+"</tweetId>";
		output+="<hashTag>"+tweetWritable.getHashtag()+"</hashTag>";
		output+="<message>"+tweetWritable.getMessage()+"</message>";
		output+="<lang>"+tweetWritable.getIdioma()+"</lang>";
		output+="<sentiment>"+tweetWritable.getSentiment()+"</sentiment>";
		output += "</tweet>";
		
		return output;
	}
}
