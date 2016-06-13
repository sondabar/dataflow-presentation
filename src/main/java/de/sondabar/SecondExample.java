package de.sondabar;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TextualIntegerCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Sum;

public class SecondExample {

    public static void main(String[] args) {
        final DataflowPipelineOptions options = PipelineOptionsFactory.create()
                                                                      .as(DataflowPipelineOptions.class);
        options.setRunner(DirectPipelineRunner.class);

        final Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(
                TextIO.Read.from("src/main/resources/integers.csv").withCoder(TextualIntegerCoder.of()))
                .apply(Sum.integersGlobally())
                .apply(TextIO.Write.to("src/main/resources/sumIntegers.csv")
                                   .withCoder(TextualIntegerCoder.of()));

        pipeline.run();

    }
}