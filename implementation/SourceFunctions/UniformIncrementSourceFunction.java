package ch.ethz.systems.strymon.ds2.flink.wordcount.sources;

import ch.ethz.systems.strymon.ds2.common.RandomSentenceGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UniformIncrementSourceFunction extends RichParallelSourceFunction<String>  {

    private static final Logger logger  = LoggerFactory.getLogger(UniformIncrementSourceFunction.class);

    /** how many sentences to output per second for rate_1 **/
    private final int sentenceRateStart;

    /** how many sentences to output per second for rate_2 **/
    private final int timeStep;

    /** the time period for rate_1 in minutes **/
    private final int incrementSize;

    /** the length of each sentence (in chars) **/
    private final int sentenceSize;

    private final RandomSentenceGenerator generator;

    private volatile boolean running = true;

    public UniformIncrementSourceFunction(int rate, int step, int increment, int size) {
        sentenceRateStart = rate;
        timeStep = step;
        incrementSize = increment;
        generator = new RandomSentenceGenerator();
        sentenceSize = size;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        long startTime = System.currentTimeMillis();
        while (running) {
            long emitStartTime = System.currentTimeMillis();
			long runTime = ((System.currentTimeMillis() - startTime) / 1000);
 			int sentenceRate = (int) Math.round(sentenceRateStart + (Math.floor(runTime/timeStep) * incrementSize));

            logger.info("sentenceRate is {}", sentenceRate);

			for (int i = 0; i < sentenceRate; i++) {
                ctx.collect(generator.nextSentence(sentenceSize));
            }
            // Sleep for the rest of timeslice if needed
            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < 1000) {
                Thread.sleep(1000 - emitTime);
            }
        }

        ctx.close();
    }

    @Override
    public void cancel() {
        running = false;
    }
}
