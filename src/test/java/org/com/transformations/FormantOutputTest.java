package org.com.transformations;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;


public class FormantOutputTest {

	@Test
	public void test() {
		DataflowPipelineOptions option = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		option.setRunner(DirectRunner.class);
		String message="2018-03-12T17:36:52.000-0700,GA.POST,/api-gateway/v2/logs,API-Gateway";
		String key="Test";
		Long value=1000L;
		String result=key+","+value;
		
		List<String> listofMessages = new ArrayList<String>();
		listofMessages.add(message);
		try {
			final Pipeline pipeline = TestPipeline.create(option);
			PCollection<String> pcoll = pipeline
					.apply("Create", Create.of(KV.of(key, value)))
					.apply("Format Output",ParDo.of(new FormatOutput()));
					
			pcoll.apply(ParDo.of(new PrintOutput()));
			
			PAssert.that(pcoll).containsInAnyOrder(result);
			pipeline.run();
		} catch (Exception e) {
			// TODO: handle exception
			//System.out.println(e);
		}
	}

}