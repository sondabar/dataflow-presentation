package de.sondabar.fn.read;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import de.sondabar.model.BidResponse;

public class ToBidResponse extends DoFn<TableRow, BidResponse> {
    @Override
    public void processElement(ProcessContext processContext) throws Exception {
        final TableRow line = processContext.element();
        final BidResponse bidResponse = new BidResponse(line);
        processContext.output(bidResponse);
    }
}
