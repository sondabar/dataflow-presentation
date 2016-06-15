package de.sondabar.fn.transform;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import de.sondabar.model.CampaignResult;

public class CampaignResultToTableRows extends SimpleFunction<CampaignResult, KV<Long, TableRow>>
{
   @Override
   public KV<Long, TableRow> apply(final CampaignResult aInput)
   {
      return aInput.getTableRow();
   }
}
