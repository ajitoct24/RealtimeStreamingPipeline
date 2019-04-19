package org.com.transformations;

import org.apache.beam.sdk.transforms.DoFn;
import org.com.pipeline.DataflowStreamingPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;
public class ParseMessage extends DoFn<String, Map<String, String>> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(DataflowStreamingPipeline.class);
	@ProcessElement
	public void processElement(ProcessContext c) {
		String inputMessage = c.element().toString();
		try{
			LOG.info("Message received : "+inputMessage);
			String[] splitmessage = inputMessage.split(",");
			Map<String, String> parsedMessage=new HashMap<String,String>();
			parsedMessage.put("timestamp",splitmessage[0].substring(0, splitmessage[0].lastIndexOf("-")).trim());
			parsedMessage.put("type",splitmessage[1].trim());
			parsedMessage.put("apiname",splitmessage[2].split("/")[3].trim());
			LOG.info("Message parsed : "+parsedMessage);
			System.out.println("Message parsed : "+parsedMessage);
			c.output(parsedMessage);
		}catch(Exception e)
		{
			LOG.error("Unable to parse message : "+inputMessage);
			
		}
	}
}
