package de.sondabar.fn.read;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import de.sondabar.model.Conversion;

public class ToConversion extends DoFn<String, KV<String, Conversion>> {
    @Override
    public void processElement(ProcessContext processContext) throws Exception {
        final String line = processContext.element();
        final Conversion conversion = new Conversion(line);
        processContext.output(KV.of(conversion.getBidCidKey(), conversion));
    }
}
