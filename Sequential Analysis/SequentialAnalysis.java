import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

/**
 * 
 * Sort airlines based on average price cost
 * @author rachit puri
 *
 */
public class SequentialAnalysis {
	
	// Constants
	private static final String FILENAME = "323.csv.gz";

	// data members
	private int valid = 0;
	private int invalid = 0;
	
	public static HashMap<String, Float> unsorted = new HashMap<String, Float>();
	public static HashMap<String, Integer> airline = new HashMap<String, Integer>();
	
	/**
	 * 
	 * @param time
	 * @return
	 */
	private int timeToMinute(int time) {
		String timeVal = String.valueOf(time);
		StringBuilder sb = new StringBuilder(timeVal);
		int len = timeVal.length();
		int zeros = 4 - len;
		StringBuilder z = new StringBuilder();
		while (zeros != 0) {
			z.append('0');
			zeros--;
		}
		String add = z.toString();
		sb.insert(0, add);
		String correct_format = sb.toString();
		int minutes = Integer.valueOf(correct_format.substring(0, 2))*60 + Integer.valueOf(correct_format.substring(2, 4));
		return minutes;
	}
	
	/**
	 * 
	 * @param content
	 * @return
	 */
	private boolean validData(String content) {
		boolean validData = true;
		content = content.replaceAll("\"", "");
		String formatedData = content.replaceAll(", ", ":");
		String[] data = formatedData.split(",");
		if (data.length != 110)
			return false;
		
		try {
			int CRSArrTime = Integer.valueOf(data[40]);
			int CRSDepTime = Integer.valueOf(data[29]);
			int timeZone = timeToMinute(Integer.valueOf(data[40])) - timeToMinute(Integer.valueOf(data[29])) - Integer.valueOf(data[50]);
			int origin_AirportID = Integer.valueOf(data[11]);
			int dest_AirportID = Integer.valueOf(data[20]);
			int origin_AirportSeqID = Integer.valueOf(data[12]);
			int dest_AirportSeqID = Integer.valueOf(data[21]);
			int origin_CityMarketID = Integer.valueOf(data[13]);
			int dest_CityMarketID = Integer.valueOf(data[22]);
			int origin_StateFips = Integer.valueOf(data[17]);
			int dest_StateFips = Integer.valueOf(data[26]);
			int origin_wac = Integer.valueOf(data[19]);
			int dest_wac = Integer.valueOf(data[28]);
			int cancelled = Integer.valueOf(data[47]);
			String origin = data[14];
			String dest = data[23];
			String origin_cityname = data[15];
			String dest_cityname = data[24];
			String origin_state = data[18];
			String dest_state = data[27];
			String origin_state_abr = data[16];
			String dest_state_abr = data[25];
			
			if ((CRSArrTime != 0 && CRSDepTime != 0) &&
				(timeZone%60 == 0) &&	
				(origin_AirportID >0 && dest_AirportID > 0 && origin_AirportSeqID > 0 && dest_AirportSeqID > 0 
						&& origin_CityMarketID > 0 && dest_CityMarketID > 0 && origin_StateFips > 0 && dest_StateFips > 0 
						&& origin_wac > 0 && dest_wac > 0)	&&
				(!origin.isEmpty() && !dest.isEmpty() && !origin_cityname.isEmpty() && !dest_cityname.isEmpty()
						&& !origin_state.isEmpty() && !dest_state.isEmpty() && !origin_state_abr.isEmpty() && !dest_state_abr.isEmpty())) {
				
				// for flights which are not cancelled
				if (cancelled != 1) {
					if (((timeToMinute(Integer.valueOf(data[41])) - timeToMinute(Integer.valueOf(data[30])) - Integer.valueOf(data[51]) - timeZone)/60)%24 == 0) {
						if (Float.valueOf(data[42]) > 0) {
							if (Float.valueOf(data[42]).equals(Float.valueOf(data[43])))
								validData = true;
							else
								return false;
						}
						
						if (Float.valueOf(data[42]) < 0) {
							if (Float.valueOf(data[43]) == 0)
								validData = true;
							else
								return false;
						}
						
						if (Float.valueOf(data[43]) >= 15) {
							if (Float.valueOf(data[44]) == 1)
								validData = true;
							else
								return false;
						}
					} else
						return false;
				}
			} else {
				return false;
			}
		} catch (NumberFormatException e) {
			return false;
		}
		
		// fill map
		try {
			String carrier = data[8];
			float price = Float.valueOf(data[109]);
			
			if (airline.containsKey(carrier)) {
				int count = airline.get(carrier);
				airline.put(carrier, count + 1);
			} else {
				airline.put(carrier, 1);
			}
			
			if (unsorted.containsKey(carrier)) {
				float previous_price = unsorted.get(carrier);
				unsorted.put(carrier, price + previous_price);
			} else
				unsorted.put(carrier, price);
		} catch (NumberFormatException e) {
		}
		
		return validData;
	}
	
	/**
	 * 
	 * @param unsorted
	 * @return
	 */
	public static HashMap<String, Float> sortedMap(HashMap<String, Float> unsorted) {
		List<Map.Entry<String, Float>> list = new ArrayList<Map.Entry<String, Float>>(unsorted.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Float>>() {

			@Override
			public int compare(Entry<String, Float> o1, Entry<String, Float> o2) {
				float val1 = o1.getValue();
				float val2 = o2.getValue();		
				if (val1 > val2) {
					return 1;
				} else if (val1 < val2) {
					return -1;
				} else {
					return 0;
				}
			}
		});
		HashMap<String, Float> sortedMap = new LinkedHashMap<String, Float>();
		for (Iterator<Entry<String, Float>> it = list.iterator(); it.hasNext();) {
			Map.Entry<String, Float> entry = (Entry<String, Float>) it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
	
	public static void main(String[] args) throws IOException {
		//System.out.println(System.getProperty("user.dir"));
		SequentialAnalysis sq = new SequentialAnalysis();
		GZIPInputStream gzip = null;
		try {
			gzip = new GZIPInputStream(new FileInputStream(FILENAME));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
		String content;
		while ((content = br.readLine()) != null){
			// perform sanity test
			if (sq.validData(content)) {
				sq.setValid(sq.getValid() + 1);
			} else {
				sq.setInvalid(sq.getInvalid() + 1);
			}
		}
		
		for (Map.Entry<String, Float> entry : unsorted.entrySet()) {
			int number = airline.get(entry.getKey());
			unsorted.put(entry.getKey(), entry.getValue()/number);
		}
		
		System.out.println(sq.getInvalid());
		System.out.println(sq.getValid());
		HashMap<String, Float> result = sortedMap(unsorted);
		for (Map.Entry<String, Float> entry : result.entrySet()) {
			System.out.println(entry.getKey()+" "+entry.getValue());
		}
	}

	public int getValid() {
		return valid;
	}

	public void setValid(int valid) {
		this.valid = valid;
	}

	public int getInvalid() {
		return invalid;
	}

	public void setInvalid(int invalid) {
		this.invalid = invalid;
	}
}
