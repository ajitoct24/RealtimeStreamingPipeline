package org.com.pipeline;

import java.util.Map;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.com.transformations.AssignKey;
import org.com.transformations.FormatOutput;
import org.com.transformations.ParseMessage;
import org.com.util.ReadConfig;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */

public class DataflowStreamingPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(DataflowStreamingPipeline.class);

	public void initializePipelintOptions(DataflowPipelineOptions pipelineOptions, ReadConfig config){
		
		pipelineOptions.setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
		int maxNumWorkers = Integer.parseInt(config.getProperty("maxnumworkers"));
		pipelineOptions.setMaxNumWorkers(maxNumWorkers);
		if(config.getProperty("workermachinetype")!=null){
			pipelineOptions.setWorkerMachineType(config.getProperty("workermachinetype"));
		}
		//pipelineOptions.setUsePublicIps(false);
		pipelineOptions.setProject(config.getProperty("projectid"));
		pipelineOptions.setJobName(	("pbsb-" + config.getProperty("subscriptionname") + "-gcs-" + config.getProperty("gcsbucket"))
						.replace("_", ""));
		if (config.getProperty("runner").equals("DataflowRunner")) {
			pipelineOptions.setRunner(DataflowRunner.class);
		} else {
			 pipelineOptions.setRunner(DirectRunner.class);
		}
		pipelineOptions.setStreaming(true);
		pipelineOptions.setTempLocation(config.getProperty("temp"));
		pipelineOptions.setStagingLocation(config.getProperty("staging"));
	}

	public void run(ReadConfig config)  {

		// Start by defining the options for the pipeline.
		DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		initializePipelintOptions(pipelineOptions,config);

		//Create pipeline from pipeline options
		Pipeline p = Pipeline.create(pipelineOptions);
		
		//Read messages from Pubsub
		PCollection<String> inLogsFromPubSub = p.apply("Read From Streaming",PubsubIO.readStrings()
				.fromSubscription(StaticValueProvider.of("projects/" + config.getProperty("projectid")
						+ "/subscriptions/" + config.getProperty("subscriptionname"))));
		//Parse received messages
		PCollection<Map<String, String>> parsedMessage = inLogsFromPubSub
				.apply("Parse Message",ParDo.of(new ParseMessage()));
		
		//For 1 min window process count
		parsedMessage.apply("1 Minute Window",
				Window.<Map<String, String>>into(FixedWindows.of(Duration.standardMinutes(1)))
			      .triggering(
			          AfterWatermark.pastEndOfWindow()
			              .withEarlyFirings(AfterProcessingTime
			                  .pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1))))
			      .discardingFiredPanes()
			      .withAllowedLateness(Duration.standardMinutes(1)))
				.apply("Assign key 1 min",ParDo.of(new AssignKey(1)))				
				.apply("Count by key 1 min",Count.<String>perElement())
				.apply("Format Result 1 min",ParDo.of(new FormatOutput()))
				.apply("Write to GCS 1 min",TextIO.write().to(config.getProperty("gcspath")+"-1min-").withWindowedWrites().withNumShards(1));
		
		//For 5min window process aggregations
		parsedMessage.apply("5 Minute Window",
				Window.<Map<String, String>>into(FixedWindows.of(Duration.standardMinutes(5)))
			      .triggering(
			          AfterWatermark.pastEndOfWindow()
			              .withEarlyFirings(AfterProcessingTime
			                  .pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(5))))
			      .discardingFiredPanes()
			      .withAllowedLateness(Duration.standardMinutes(5)))
				.apply("Assign key 5 min",ParDo.of(new AssignKey(5)))				
				.apply("Count by key 5 min",Count.<String>perElement())
				.apply("Format Result 5 min",ParDo.of(new FormatOutput()))
				.apply("Write to GCS 5 min",TextIO.write().to(config.getProperty("gcspath")+"-5min-").withWindowedWrites().withNumShards(1));
		LOG.info("Launching pipeline..");
		p.run();
	}
	
	public static void main(String[] args) {
		//Read config file
		Map<String, String> arguments = ReadConfig.parseArguments(args);
		if (!arguments.containsKey("config")) {
			try {
				throw new Exception("Invalid arguments");
			} catch (Exception e) {				
				LOG.error("Error occured in Read Config"+e);
			}
		}
		final ReadConfig config = new ReadConfig(arguments.get("config"));
		//Create and run dataflow
		new DataflowStreamingPipeline().run(config);
	}

}
