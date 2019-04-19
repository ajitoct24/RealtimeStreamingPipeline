package org.com.transformations;

import org.apache.beam.sdk.transforms.DoFn;
import org.com.pipeline.DataflowStreamingPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
public class AssignKey extends DoFn<Map<String, String>, String> {
	private static final Logger LOG = LoggerFactory.getLogger(DataflowStreamingPipeline.class);

	private static final long serialVersionUID = 1L;
	int windowsize;
	public AssignKey(int windowsize) {
		this.windowsize=windowsize;
	}
	@SuppressWarnings("deprecation")
	@ProcessElement
	public void processElement(ProcessContext c) {
		Map<String, String> inputMessageMap = c.element();
		final String TIMEFORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
		DateFormat formatter = new SimpleDateFormat(TIMEFORMAT);
		try {
			Date timestamp = formatter.parse(inputMessageMap.get("timestamp"));
			timestamp.setMinutes(((int)timestamp.getMinutes()/windowsize)*windowsize);
			timestamp.setSeconds(0);
			String key=timestamp.toString()+","+inputMessageMap.get("apiname")+","+inputMessageMap.get("type");
			c.output(key);
		} catch (ParseException e) {
			LOG.error("Error in AssignKey"+e);
		}
		
	}
}
