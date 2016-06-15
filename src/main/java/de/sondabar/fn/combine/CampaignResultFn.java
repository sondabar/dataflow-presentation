package de.sondabar.fn.combine;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.values.KV;
import de.sondabar.common.TupleTags;
import de.sondabar.model.Bid;
import de.sondabar.model.CampaignResult;
import de.sondabar.model.CampaignResult.State;
import de.sondabar.model.Click;
import de.sondabar.model.Conversion;
import de.sondabar.model.Event;
import de.sondabar.model.Impression;
import de.sondabar.model.VisibleImpression;
import de.sondabar.model.WonBid;

import java.util.Stack;


public class CampaignResultFn extends DoFn<KV<String, CoGbkResult>, CampaignResult> {

    public CampaignResultFn() {
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
        CoGbkResult value = c.element().getValue();
        Bid bid = value.getOnly(TupleTags.CAMPAIGN_TUPLE);
        WonBid wonBid = value.getOnly(TupleTags.WON_BID_TUPLE, null);
        Impression impression = value.getOnly(TupleTags.IMPRESSION_TUPLE, null);
        VisibleImpression visibleImpression = value.getOnly(TupleTags.VIS_IMPRESSION_TUPLE, null);
        Click click = value.getOnly(TupleTags.CLICK_TUPLE, null);
        Conversion conversion = value.getOnly(TupleTags.CONVERSION_TUPLE, null);

        CampaignResult campaignResult = new CampaignResult(bid, wonBid);
        campaignResult.setState(getState(bid, wonBid, impression, visibleImpression, click, conversion));
        c.output(campaignResult);
    }

    private State getState(Event... events) {
        Stack<Event> stack = new Stack<>();
        for (Event event : events) {
            if (event != null) {
                stack.push(event);
            }
        }
        return State.valueOf(stack.pop().getClass().getSimpleName());
    }
}
