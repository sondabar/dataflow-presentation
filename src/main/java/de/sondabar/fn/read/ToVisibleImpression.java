package de.sondabar.fn.read;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import de.sondabar.model.VisibleImpression;

public class ToVisibleImpression extends DoFn<String, KV<String, VisibleImpression>> {
    @Override
    public void processElement(ProcessContext processContext) throws Exception {
        final String line = processContext.element();
        final VisibleImpression impression = new VisibleImpression(line);
        processContext.output(KV.of(impression.getBidCidKey(), impression));
    }
}
