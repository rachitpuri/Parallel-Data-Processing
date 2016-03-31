import scala.collection.mutable.ListBuffer

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import scala.collection.JavaConversions._

// Author: Adib Alwani and Rachit Puri
class RFastMedian extends Reducer[Text, FloatWritable, Text, FloatWritable] {
	type Context = Reducer[Text, FloatWritable, Text, FloatWritable]#Context


/*
		 * This program determines the kth order statistic (the kth largest number in a
         * list) in O(n) time in the average case and O(n^2) time in the worst case.  It
         * achieves this through the Quickselect algorithm.
		 * 
		 */

		 def quickSelect(medianvalues: ListBuffer[Float], k : Int) : Float = {


		 	var result = kthLargest(medianvalues, 0, medianvalues.length, k);
		 	return result;
		 }


			/**
     		* Recursively determines the kth order statistic for the given list.
     		*
     		* @param medianvalues          The list.
     		* @param low                   The left index of the current sublist.
     		* @param high                  The right index of the current sublist.
     		* @param k                     The k value to use.
     		* @return                      The kth order statistic for the list.
     		*/
     		
     		def kthLargest(medianvalues : ListBuffer[Float], low : Int, high: Int, k : Int) : Float = {

     			if (low < high) {
     				var pivotLoc = partition(medianvalues, low, high);
     				var temp = medianvalues(pivotLoc);
     				medianvalues(pivotLoc) = medianvalues(low);
     				medianvalues(low) = temp;
     				if (pivotLoc == k - 1) {

     					return medianvalues(pivotLoc);

     					} else if (pivotLoc > k - 1) {
     						return kthLargest(medianvalues, low, pivotLoc, k);
     						} else {
     							return kthLargest(medianvalues, pivotLoc + 1, high, k);
     						}
     					}

     					return -1;

     				}

					 /**
                      * Randomly partitions a set about a pivot such that the values to the left
                      * of the pivot are less than or equal to the pivot and the values to the
                      * right of the pivot are greater than the pivot.
                      *
                      * @param values          The list.
                      * @param low             The left index of the current sublist.
                      * @param high            The right index of the current sublist.
                      * @return                The index of the pivot.
                      */


                      def partition(values : ListBuffer[Float], low : Int, high : Int) : Int = {


                      	var pivot = values(low);
                      	var left = low;

                      	var i=0;

                      	for (i <- (low + 1) until high) {
                      		if (values(i) < pivot) {
                      			left +=  1;
                      			var temp = values(i);
                      			values(i) = values(left);
                      			values(left) = temp;
                      		}
                      	}

                      	return left;
                      }


	/*def partition (arr : ListBuffer[Float], low : Int, high : Int) : Int = {
		var pivot = arr(low)
		var left = low
		var i = low + 1
println("EnteringP")
		for (i <- low + 1 until high) {
			if (arr(i) < pivot) {
				left += 1
				var temp = arr(i)
//				arr.update(i, arr(left))
//				arr.update(left, temp)
				arr(i) = arr(left)
				arr(left) = temp
			}
		}
println("ExitingP")
		return left
	}

	def kthLargest (arr : ListBuffer[Float], low : Int, high : Int, k : Int) : Float = {
		if (low < high) {
			var pivotLoc = partition(arr, low, high)
			var temp = arr(pivotLoc)
//			arr.update(pivotLoc, arr(low))
//			arr.update(low, temp)
			arr(pivotLoc) = arr(low)
			arr(low) = temp
			if (pivotLoc == k - 1) {
				println(arr(pivotLoc).toString())
				return arr(pivotLoc)
			} else if (pivotLoc > k - 1) {
				return kthLargest(arr, low, pivotLoc, k)
			} else {
				return kthLargest(arr, pivotLoc + 1, high, k)
			}
		}

		return -1
	}*/

	/*def partition (arr : Array[Float], low : Int, high : Int) : Int = {
		var pivot = arr(low)
		var left = low
		var i = 0
println("EnteringP")
		for (i <- (low + 1) until high) {
			/*if (arr(i) < pivot) {
				left += 1
				var temp = arr(i)
				arr(i) = arr(left)
				arr(left) = temp
			}*/
		}
println("ExitingP")
		return left
	}

	def kthLargest (arr : Array[Float], low : Int, high : Int, k : Int) : Float = {
		if (low < high) {
			var pivotLoc = partition(arr, low, high)
			var temp = arr(pivotLoc)
			arr(pivotLoc) = arr(low)
			arr(low) = temp
			if (pivotLoc == k - 1) {
				return arr(pivotLoc)
			} else if (pivotLoc > k - 1) {
				return kthLargest(arr, low, pivotLoc, k)
			} else {
				return kthLargest(arr, pivotLoc + 1, high, k)
			}
		}

		return -1
	}*/

	override def reduce(key : Text, values: java.lang.Iterable[FloatWritable], context: Context) {
		var prices = new ListBuffer[Float]()
		for (price <- values) {
			prices += price.get()
		}
		prices = prices.sorted
		//var medianPrice = 0.0
		var arr = new Array[Float](prices.length)
		prices.copyToArray(arr)
		var count = prices.length



				var priceSize  = prices.length;

				var medianPrice=0.0;
				if(priceSize % 2 == 0){

					medianPrice = (quickSelect(prices, (priceSize/2)) +  quickSelect(prices, (priceSize/2 - 1)))/ 2;

				}
				else{
					medianPrice = quickSelect(prices, (priceSize/2 - 1));
				}

				context.write(key, new Text(count + "\t" + String.valueOf(medianPrice)));

		
		/*println("Entering")
		if (count % 2 == 0) {
			medianPrice = (kthLargest(prices, 0, count, count / 2) + kthLargest(prices, 0, count, count / 2 - 1)) / 2
		} else {
			medianPrice = kthLargest(prices, 0, count, count / 2 - 1)
		}
		println("Done")
		context.write(new Text(key.toString() + "\t" + count), new FloatWritable(medianPrice.toFloat))*/
	}

}
