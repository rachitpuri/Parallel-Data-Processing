import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Mapper class to test the built model
 * 
 * @author Adib Alwani and Rachit Puri
 */
public class TestMapper extends Mapper<Object, Text, Text, FlightDetail> {

	@Override
	public void map(Object key, Text value, Context context) 
		throws IOException, InterruptedException {
		
		FlightHandler handler = new FlightHandler();
		String[] row = handler.parse(value.toString(), 112);
		
		if (row != null) {
			row = Arrays.copyOfRange(row, 1, row.length);
			if (handler.sanityTest(row, FlightHandler.TEST)) {
				FlightDetail flightDetail = handler.getFlightDetails(row);
				String month = row[2];
				int flightNumber = (int) Float.parseFloat(row[10]);
				flightDetail.setFlightNumber(new IntWritable(flightNumber));
				String flightDate = row[5];
				flightDetail.setFlightDate(new Text(flightDate));
				
				context.write(new Text(month), flightDetail);
			}
		}
	}
}
