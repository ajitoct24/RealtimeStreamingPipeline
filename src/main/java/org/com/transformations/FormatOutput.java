package org.com.transformations;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.com.pipeline.DataflowStreamingPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class FormatOutput extends DoFn<KV<String, Long>, String> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(DataflowStreamingPipeline.class);
	@ProcessElement
	public void processElement(ProcessContext c) {
		KV<String, Long> inputMessageMap = c.element();
		String result=inputMessageMap.getKey()+","+inputMessageMap.getValue();
		LOG.info("Result : "+result);
		
		c.output(result);
	}
}
