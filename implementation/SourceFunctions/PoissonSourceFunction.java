package ch.ethz.systems.strymon.ds2.flink.wordcount.sources;

import ch.ethz.systems.strymon.ds2.common.RandomSentenceGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PoissonSourceFunction extends RichParallelSourceFunction<String> {

	private static final Logger logger  = LoggerFactory.getLogger(PoissonSourceFunction.class);
    /** how many sentences to output per second **/
    private final int sentenceRate;

    /** the length of each sentence (in chars) **/
    private final int sentenceSize;

    private final RandomSentenceGenerator generator;

    private volatile boolean running = true;

    public PoissonSourceFunction(int rate, int size) {
        sentenceRate = rate;
        generator = new RandomSentenceGenerator();
        sentenceSize = size;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        double L = 1.0 / sentenceRate;
		long startTime = System.currentTimeMillis();
        while (running) {
			long emitStartTime = System.currentTimeMillis();
			long runTime = ((System.currentTimeMillis() - startTime) / 1000);
			
            int arrivals = (int) Math.round(Math.log(1.0-Math.random())/-L);

			logger.info("arrivals is {}", arrivals);
			
			for (int i = 0; i < arrivals; i++) {
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
