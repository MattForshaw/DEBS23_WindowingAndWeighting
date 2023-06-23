package ch.ethz.systems.strymon.ds2.flink.wordcount.sources;

import ch.ethz.systems.strymon.ds2.common.RandomSentenceGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SineWaveSourceFunction extends RichParallelSourceFunction<String> {

    private static final Logger logger  = LoggerFactory.getLogger(SineWaveSourceFunction.class);
	
	private final int amp;

	private final double omega;
	
	private final int H_STEP_SIZE;
	
	private final int ph_shift;
	
	private final int vert_shift;

    private int sentenceRate;
	 
    /** the length of each sentence (in chars) **/
    private final int sentenceSize;

    private final RandomSentenceGenerator generator;

    private volatile boolean running = true;

    public SineWaveSourceFunction(int ampl, double freq, int step, int phase_shift, int vertical_shift, int size) {
		amp = ampl;
		omega = freq;
		H_STEP_SIZE = step;
		ph_shift = phase_shift;
		vert_shift = vertical_shift;
        generator = new RandomSentenceGenerator();
        sentenceSize = size;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        logger.info("Blah2!");
        long startTime = System.currentTimeMillis();
        while (running) {
            long emitStartTime = System.currentTimeMillis();
			long runTime = ((System.currentTimeMillis() - startTime) / 1000);
			int sentenceRate = (int) Math.round((amp *(Math.sin(omega*((runTime/H_STEP_SIZE) + ph_shift)))) + vert_shift);

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

