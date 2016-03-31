import scala.collection.mutable.ListBuffer

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import scala.collection.JavaConversions._

// Author: Adib Alwani and Rachit Puri
class RMedian extends Reducer[Text, FloatWritable, Text, FloatWritable] {
	type Context = Reducer[Text, FloatWritable, Text, FloatWritable]#Context

	override def reduce(key : Text, values: java.lang.Iterable[FloatWritable], context: Context) {
		var prices = new ListBuffer[Float]()
		var count = 0
		for (price <- values) {
			prices += price.get()
			count += 1
		}
		prices = prices.sorted
		var medianPrice = 0.0;
		if (count % 2 == 0) {
			medianPrice = (prices(count / 2) + prices(count / 2 - 1)) / 2
		} else {
			medianPrice = prices(count / 2 - 1)
		}
		context.write(new Text(key.toString() + "\t" + count), new FloatWritable(medianPrice.toFloat))
	}

}
