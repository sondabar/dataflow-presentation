package de.sondabar.fn.read;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import de.sondabar.model.Bid;
import de.sondabar.model.BidLine;
import de.sondabar.model.BidResponse;

public class ToCampaignResponse extends DoFn<BidResponse, KV<String, Bid>> {
    @Override
    public void processElement(ProcessContext processContext) throws Exception {
        final BidResponse bidResponse = processContext.element();
        for(BidLine bidLine : bidResponse.getBidLines() )
        {
            Bid response = new Bid(bidResponse, bidLine.getCid());
            processContext.output(KV.of(response.getBidCidKey(), response));
        }
    }
}
