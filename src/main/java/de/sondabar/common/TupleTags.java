package de.sondabar.common;

import com.google.cloud.dataflow.sdk.values.TupleTag;
import de.sondabar.model.*;

public class TupleTags {
    public static final TupleTag<Bid> CAMPAIGN_TUPLE = new TupleTag<>();
    public static final TupleTag<WonBid> WON_BID_TUPLE = new TupleTag<>();
    public static final TupleTag<Impression> IMPRESSION_TUPLE = new TupleTag<>();
    public static final TupleTag<VisibleImpression> VIS_IMPRESSION_TUPLE = new TupleTag<>();
    public static final TupleTag<Click> CLICK_TUPLE = new TupleTag<>();
    public static final TupleTag<Conversion> CONVERSION_TUPLE = new TupleTag<>();
}
