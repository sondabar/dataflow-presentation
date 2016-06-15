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
    public void processElement(final ProcessContext c) throws Exception {
        final CoGbkResult value = c.element().getValue();
        final Bid bid = value.getOnly(TupleTags.CAMPAIGN_TUPLE);
        final WonBid wonBid = value.getOnly(TupleTags.WON_BID_TUPLE, null);
        final Impression impression = value.getOnly(TupleTags.IMPRESSION_TUPLE, null);
        final VisibleImpression visibleImpression = value.getOnly(TupleTags.VIS_IMPRESSION_TUPLE, null);
        final Click click = value.getOnly(TupleTags.CLICK_TUPLE, null);
        final Conversion conversion = value.getOnly(TupleTags.CONVERSION_TUPLE, null);

        final CampaignResult campaignResult = new CampaignResult(bid, wonBid);
        campaignResult.setState(getState(bid, wonBid, impression, visibleImpression, click, conversion));
        c.output(campaignResult);
    }

    private State getState(final Event... events) {
        final Stack<Event> stack = new Stack<>();
        for (final Event event : events) {
            if (event != null) {
                stack.push(event);
            }
        }
        return State.valueOf(stack.pop().getClass().getSimpleName());
    }
}
