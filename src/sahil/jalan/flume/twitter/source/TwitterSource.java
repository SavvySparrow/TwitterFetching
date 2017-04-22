package sahil.jalan.flume.twitter.source;

/**
 * Created by sahiljalan on 16/4/17.
 */

import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.*;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class TwitterSource extends AbstractSource
        implements EventDrivenSource, Configurable {

    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;

    private String[] keywords;

    /** The actual Twitter stream. It's set up to collect raw JSON data */
    private  TwitterStream twitterStream;

    /**
     * The initialization method for the Source. The context contains all the
     * Flume configuration info, and can be used to retrieve any configuration
     * values necessary to set up the Source.
     */
    @Override
    public void configure(Context context) {
        consumerKey = context.getString(TwitterSourceConstant.CONSUMER_KEY);
        consumerSecret = context.getString(TwitterSourceConstant.CONSUMER_SECRET_KEY);
        accessToken = context.getString(TwitterSourceConstant.ACCESS_TOKEN);
        accessTokenSecret = context.getString(TwitterSourceConstant.ACCESS_TOKEN_SECRET);

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey(consumerKey);
        cb.setOAuthConsumerSecret(consumerSecret);
        cb.setOAuthAccessToken(accessToken);
        cb.setOAuthAccessTokenSecret(accessTokenSecret);
        cb.setJSONStoreEnabled(true);

        String keywordString = context.getString(TwitterSourceConstant.KEYWORDS,TwitterSourceConstant.DEFAULT_KEYWORD);
        //keywords = keywordString.replace(TwitterSourceConstant.COMMA_CHARACTER, TwitterSourceConstant.OR_CHARACTER);

        if (keywordString.trim().length() == 0) {
            keywords = new String[0];
        } else {
            keywords = keywordString.split(",");
            for (int i = 0; i < keywords.length; i++) {
                keywords[i] = keywords[i].trim();
            }
        }
        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
    }

    @Override
    public void start(){
        // The channel is the piece of Flume that sits between the Source and Sink,
        // and is used to process events.
        final ChannelProcessor channel = getChannelProcessor();

        final Map<String, String> headers = new HashMap<String, String>();

        // The StatusListener is a twitter4j API, which can be added to a Twitter
        // stream, and will execute methods every time a message comes in through
        // the stream.
        StatusListener statusListener = new StatusListener() {
            // The onStatus method is executed every time a new tweet comes in.
            @Override
            public void onStatus(Status status) {
                // The EventBuilder is used to build an event using the headers and
                // the raw JSON of a tweet
                // shouldn't log possibly sensitive customer data
                //logger.debug("tweet arrived");

                headers.put("timestamp",String.valueOf(status.getCreatedAt().getTime()));

                Event event1;
                event1 = EventBuilder.withBody(TwitterObjectFactory.getRawJSON(status).getBytes(),headers);
                //event2 = EventBuilder.withBody(Integer.toString(status.getRetweetCount()),(Charset)headers);
                //event3 = EventBuilder.withBody(status.getText().getBytes(), headers);
                channel.processEvent(event1);
                //channel.processEvent(event2);

            }

            @Override public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }
            @Override public void onTrackLimitationNotice(int i) {

            }
            @Override public void onScrubGeo(long l, long l1) {

            }
            @Override public void onStallWarning(StallWarning stallWarning) {

            }
            @Override public void onException(Exception e) {

            }
        };

        twitterStream.addListener(statusListener);

        if(keywords.length != 0){
            FilterQuery filterQuery = new FilterQuery().track(keywords);
            twitterStream.filter(filterQuery);
        }else{
            twitterStream.sample();
        }
        super.start();
    }

    @Override
    public void stop(){
        twitterStream.shutdown();
        super.stop();
    }

}

