package de.sondabar.fn.read;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import de.sondabar.model.WonBid;

public class ToWonBid extends DoFn<String, KV<String, WonBid>> {
    @Override
    public void processElement(ProcessContext processContext) throws Exception {
        final String line = processContext.element();
        final WonBid wonBid = new WonBid(line);
        processContext.output(KV.of(wonBid.getBidCidKey(), wonBid));
    }
}
