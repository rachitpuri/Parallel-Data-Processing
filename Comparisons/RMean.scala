import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import scala.collection.JavaConversions._

// Author: Adib Alwani and Rachit Puri
class RMean extends Reducer[Text, FloatWritable, Text, FloatWritable] {
	type Context = Reducer[Text, FloatWritable, Text, FloatWritable]#Context

	override def reduce(key : Text, values: java.lang.Iterable[FloatWritable], context: Context) {
		var sum = 0.0
		var count = 0
		for (price <- values) {
			sum += price.get();
			count += 1;
		}
		var averagePrice = (sum / count.toFloat).toFloat;
		context.write(new Text(key.toString() + "\t" + count), new FloatWritable(averagePrice));
	}

}
