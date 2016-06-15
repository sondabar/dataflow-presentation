package de.sondabar.fn.transform;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;

/**
 * Documentation required !
 * <p/>
 * @author of last revision $Author: atodorov lcormann $
 * @version $Revision: 1.1 $ $Date: 2011/03/16 14:39:45 6/15/16 3:52 PM $
 */
public class TableRowsToColumns extends DoFn<KV<Long, Iterable<TableRow>>, KV<String, Long>>
{
   @Override
   public void processElement(final ProcessContext c) throws Exception
   {
      final Long key = c.element().getKey();
      for(final TableRow row : c.element().getValue())
      {
         for(final String rowKey : row.keySet())
         {
            c.output(KV.of(key + "_" + rowKey, new Long((Integer)row.get(rowKey))));
         }
      }
   }
}
