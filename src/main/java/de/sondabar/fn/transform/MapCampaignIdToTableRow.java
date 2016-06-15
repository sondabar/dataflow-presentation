package de.sondabar.fn.transform;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.values.KV;

import java.util.Map;

public class MapCampaignIdToTableRow extends SimpleFunction<KV<String, Map<String, String>>, TableRow>
{
   @Override
   public TableRow apply(final KV<String, Map<String, String>> input)
   {
      final TableRow tableRow = new TableRow();
      tableRow.putAll(input.getValue());
      tableRow.put("cid", input.getKey());
      return tableRow;
   }
}
