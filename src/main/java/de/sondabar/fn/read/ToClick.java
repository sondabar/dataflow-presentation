package de.sondabar.fn.read;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import de.sondabar.model.Click;

public class ToClick extends DoFn<String, KV<String, Click>> {
    @Override
    public void processElement(ProcessContext processContext) throws Exception {
        final String line = processContext.element();
        final Click click = new Click(line);
        processContext.output(KV.of(click.getBidCidKey(), click));
    }
}
