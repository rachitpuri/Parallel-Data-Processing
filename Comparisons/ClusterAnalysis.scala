import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path

// Author: Adib Alwani and Rachit Puri
object ClusterAnalysis {
	def main(args: Array[String]) {
		var mode = args(0)
		val job = Job.getInstance()
		job.setJar("job.jar")
		job.setMapperClass(classOf[M])
		if (mode == "1") {
			job.setReducerClass(classOf[RMean])
		} else if (mode == "2") {
			job.setReducerClass(classOf[RMedian])
		} else {
			job.setReducerClass(classOf[RFastMedian])
		}
		job.setMapOutputKeyClass(classOf[Text])
		job.setMapOutputValueClass(classOf[FloatWritable])
		job.setOutputKeyClass(classOf[Text])
		job.setOutputValueClass(classOf[FloatWritable])
		FileInputFormat.addInputPath(job, new Path(args(1)))
		FileOutputFormat.setOutputPath(job, new Path(args(2)))
		job.waitForCompletion(true)
	}
}

// Author: Adib Alwani
class M extends Mapper[Object, Text, Text, FloatWritable] {
	type Context = Mapper[Object, Text, Text, FloatWritable]#Context

	/**
	 * Parse a CSV record given as string
	 * - Remove any quotes from record
	 * - Splits on each new column
	 * 
	 * @param record The record to parse 
	 * @return Array containing those records, null if couldn't parse
	 */
	@throws(classOf[NumberFormatException])
	def parse (record : String, column : Int) : Array[String] = {
		
		var ans = new Array[String](column)
		var col = 0
		var len = record.length()
		var builder = StringBuilder.newBuilder
		var arr = record.toCharArray
		var i = 0

		for (i <- 0 until len - 1) {

			var ch = arr(i);
			
			// Remove quotes 
			if (ch == '\"') {
				//continue
			} else if (ch != ',') {
				// No Split condition
				builder.append(ch)
					
				if (i == len - 1) {
					ans(col) = builder.toString()
				}
			} else if (i + 1 < len && arr(i + 1) == ' ') {
				// Ignore condition
				builder.append(ch)
			} else {
				// Split condition
				if (i == len - 1) {
					throw new NumberFormatException()
				}
				
				ans(col) = builder.toString()
				builder.setLength(0)
				col = col + 1
			}
		}
		
		for (i <- 0 until len - 1) {
			println(ans(i))
		}

		return ans
	}

	/**
	 * Checks if the flight is on January 2015
	 * 
	 * @param row The record for a flight
	 * @return true if the flight is on given date, false otherwise
	 */
	def checkYear (row : Array[String]) : Boolean = {
		var year = (row(0)).toFloat.toInt
		
		if (year == 2015) {
			return true
		}
		
		return false
	}

	/**
	 * Convert time in hhmm format to minutes since day started
	 * 
	 * @param time The time in hhmm format
	 * @return Minutes minutes since day started
	 * @throws NumberFormatException
	 */
	@throws(classOf[NumberFormatException])
	def timeToMinute (time : String) : Int = {
		if (time == null || time.length() == 0) {
			throw new NumberFormatException()
		}
		
		var hhmm = (time).toFloat.toInt
		var hour = 0
		var minute = 0
		
		if (hhmm < 100) {
			minute = hhmm
		} else {
			hour = hhmm / 100
			minute = hhmm % 100
		}
		
		return hour * 60 + minute
	}

	/**
	 * Check whether the given record passes the sanitary test
	 * 
	 * @param row Record of flight OTP data
	 * @return true iff it passes sanity test. False, otherwise
	 */
	def sanityTest (row : Array[String]) : Boolean = {

		try {
				
			// hh:mm format
			var CRSArrTime = timeToMinute(row(40))
			var CRSDepTime = timeToMinute(row(29))
				
			// Check for zero value
			if (CRSArrTime == 0 || CRSDepTime == 0) {
				return false
			}
				
			// minutes format
			var CRSElapsedTime = (row(50)).toFloat.toInt
			var timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime
				
			// Check for modulo zero
			if (timeZone % 60 != 0) {
				return false;
			}
				
			var originAirportId = (row(11)).toFloat.toInt
			var destAirportId = (row(20)).toFloat.toInt
			var originAirportSeqId = (row(12)).toFloat.toInt
			var destAirportSeqId = (row(21)).toFloat.toInt
			var originCityMarketId = (row(13)).toFloat.toInt
			var destCityMarketId = (row(22)).toFloat.toInt
			var originStateFips = (row(17)).toFloat.toInt
			var destStateFips = (row(26)).toFloat.toInt
			var originWac = (row(19)).toFloat.toInt
			var destWac = (row(28)).toFloat.toInt
			
			// Check for Ids greater than zero
			if (originAirportId <= 0 || destAirportId <= 0 || originAirportSeqId <= 0 || 
					destAirportSeqId <= 0 || originCityMarketId <= 0 || destCityMarketId <= 0 || 
					originStateFips <= 0 || destStateFips <= 0 || originWac <= 0 || destWac <= 0) {
				return false
			}
			
			// Check for non-empty condition
			if (row(14).isEmpty() || row(23).isEmpty() || row(15).isEmpty() || row(24).isEmpty() ||
					row(16).isEmpty() || row(25).isEmpty() || row(18).isEmpty() || row(27).isEmpty()) {
				return false
			}
				
			// For flights that are not cancelled
			var cancelled = (row(47)).toFloat.toInt
			if (cancelled != 1) {
				var arrTime = timeToMinute(row(41))
				var depTime = timeToMinute(row(30))
				var actualElapsedTime = (row(51)).toFloat
				
				// Check for zero value
				var time = arrTime - depTime - actualElapsedTime - timeZone
				if (time != 0 && time % 1440 != 0) {
					return false
				}
				
				var arrDelay = (row(42)).toFloat
				var arrDelayMinutes = (row(43)).toFloat
				if (arrDelay > 0 && arrDelay != arrDelayMinutes) {
					return false
				} else if (arrDelay < 0 && arrDelayMinutes != 0) {
					return false
				}
				
				if (arrDelayMinutes >= 15 && ((row(44)).toFloat) != 1) {
					return false
				}
			}
				
		} catch {
			case exception : NumberFormatException => {
				
				return false

			}	
		}
			
		return true;

	}

	override def map(key: Object, value: Text, context: Context) {
		
		var line = value.toString().replaceAll("\"", "").replaceAll(", ", ";")
		var row = line.split(",")
		if (row.length == 110 && sanityTest(row) && checkYear(row)) {
			try {
				var carrierCode = row(8)
				var month = (row(2)).toFloat.toInt
				var averagePrice = (row(109)).toFloat
				context.write(new Text(carrierCode + "\t" + month), new FloatWritable(averagePrice));
			} catch {
				case exception : NumberFormatException => {
					// Do Nothing : Unable to parse float values
				}
			}
		}
	}
}
