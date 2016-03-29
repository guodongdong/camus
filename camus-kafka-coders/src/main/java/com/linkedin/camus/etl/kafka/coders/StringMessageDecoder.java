package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import org.apache.log4j.Logger;
import java.util.Properties;


/**
 * MessageDecoder class that will convert the payload into a String object,
 * System.currentTimeMillis() will be used to set CamusWrapper's
 * timestamp property

 * This MessageDecoder returns a CamusWrapper that works with Strings payloads,
 */
public class StringMessageDecoder extends MessageDecoder<byte[], String> {
  private static final Logger log = Logger.getLogger(StringMessageDecoder.class);

  @Override
  public void init(Properties props, String topicName) {
    this.props = props;
    this.topicName = topicName;
  }

  @Override
  public CamusWrapper<String> decode(byte[] payload) {
    long timestamp = 0;
    String payloadString;

    payloadString = new String(payload);
    log.info("StringMessageDecoderMsg:["+payloadString+"]");
    timestamp = System.currentTimeMillis();

    return new CamusWrapper<String>(payloadString, timestamp);
  }
}
