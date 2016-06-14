package de.sondabar.fn.read;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import de.sondabar.model.Impression;

public class ToImpression extends DoFn<String, KV<String, Impression>> {
    @Override
    public void processElement(ProcessContext processContext) throws Exception {
        final String line = processContext.element();
        final Impression impression = new Impression(line);
        processContext.output(KV.of(impression.getBidCidKey(), impression));
    }
}
