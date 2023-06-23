package ch.ethz.systems.strymon.ds2.flink.wordcount;

import ch.ethz.systems.strymon.ds2.flink.nexmark.sinks.DummySink;
import ch.ethz.systems.strymon.ds2.flink.wordcount.sources.EnvelopeSourceFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.dropwizard.metrics.*;
import com.codahale.metrics.*;


public class EnvelopeWordCount {

    private static final Logger logger  = LoggerFactory.getLogger(EnvelopeWordCount.class);

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);



		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		final DataStream<String> text = env.addSource(
				new EnvelopeSourceFunction(
						params.getInt("amplitude", 10000),
						params.getDouble("frequency", 0.006978),
						params.getInt("step-size", 1),
						params.getInt("phase-shift", 0),
						params.getInt("vertical-shift", 20000),
						params.getInt("sentence-size", 100)))
				.uid("sentence-source")
					.setParallelism(params.getInt("p1", 1));


		// split up the lines in pairs (2-tuples) containing:
		// (word,1)
		DataStream<Tuple2<String, Long>> counts = text.rebalance()
				.flatMap(new Tokenizer())
				.name("Splitter FlatMap")
				.uid("flatmap")
					.setParallelism(params.getInt("p2", 1))
				.keyBy(0)
				.flatMap(new CountWords())
				.name("Count")
				.uid("count")
					.setParallelism(params.getInt("p3", 1));

		GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
		// write to dummy sink
		counts.transform("Latency Sink", objectTypeInfo,
				new DummySink<>())
				.uid("dummy-sink")
				.setParallelism(params.getInt("p3", 1));

        logger.info("params are {}", params);
        logger.info("params amplitude are {}", params.getInt("amplitude"));

		// execute program
		env.execute("Envelope WordCount");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	public static final class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Long>> {

	    private transient Meter meter;

		private static final long serialVersionUID = 1L;

		@Override
		public void open(Configuration parameters) throws Exception {

			// add throughput meter
			com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();

			this.meter = getRuntimeContext()
			  .getMetricGroup()
			  .meter("myMeter", new DropwizardMeterWrapper(dropwizardMeter));
		}


		@Override
		public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {

			// normalize and split the line

			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
				    this.meter.markEvent();
					out.collect(new Tuple2<>(token, 1L));
				}
			}
		}
	}

	public static final class CountWords extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {

		private transient Meter meter;

		// private transient Histogram histogram;

		private transient ReducingState<Long> count;

		@Override
		public void open(Configuration parameters) throws Exception {

			ReducingStateDescriptor<Long> descriptor =
					new ReducingStateDescriptor<Long>(
							"count", // the state name
							new Count(),
							BasicTypeInfo.LONG_TYPE_INFO);

			count = getRuntimeContext().getReducingState(descriptor);

			// add throughput meter
			com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();

			this.meter = getRuntimeContext()
			  .getMetricGroup()
			  .meter("myMeter", new DropwizardMeterWrapper(dropwizardMeter));

            // add DropWizard Histogram
			// com.codahale.metrics.Histogram dropwizardHistogram =
            //    new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));
            // this.histogram = getRuntimeContext()

            // .getMetricGroup()
            // .histogram("myHistogram", new DropwizardHistogramWrapper(dropwizardHistogram));

		}

		@Override
		public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {
            this.meter.markEvent();
            // logger.info("This is the getRate meter value: {}", this.meter.getRate());

			count.add(value.f1);
			// this.histogram.update(count.get());

			out.collect(new Tuple2<>(value.f0, count.get()));
		}

		public static final class Count implements ReduceFunction<Long> {

			@Override
			public Long reduce(Long value1, Long value2) throws Exception {
				return value1 + value2;
			}
		}
	}

}
