package de.sondabar;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FirstExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(FirstExample.class);

    public static void main(String[] args) {
        final DataflowPipelineOptions options = PipelineOptionsFactory.create()
                                                                      .as(DataflowPipelineOptions.class);
        options.setRunner(DirectPipelineRunner.class);

        final Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(Create.of(1, 2, 4, 5))
                .apply(Sum.integersGlobally()).apply(ParDo.of(
                new DoFn<Integer, Integer>() {
                    @Override
                    public void processElement(ProcessContext c) throws Exception {
                        LOGGER.info(c.element().toString());
                    }
                }));

        pipeline.run();

    }
}