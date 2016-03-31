import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;


/**
 * Class to parallelly analyze data-set of 
 * The Bureau of Transport Statistics' On-time Performance (OTP)
 * 
 * @author Adib
 */
public class ThreadedAnalysis {
	
	/*
	 * Mode for computing Statistics
	 */
	private static final int MODE_MEAN = 1;
	private static final int MODE_MEDIAN = 2;
	private static final int MODE_FAST_MEDIAN = 3;
	
	/*
	 * Map containing flight information for each carrier
	 */
	private static Map<String, List<PriceDate>> flights = new HashMap<String, List<PriceDate>>();
	
	/**
	 * Class containing price, month and year
	 * 
	 * @author Adib
	 */
	static class PriceDate implements Comparable<PriceDate> {
		private float price;
		private int month;
		
		public PriceDate(float price, int month) {
			this.price = price;
			this.month = month;
		}
		
		@Override
		public int compareTo(PriceDate priceDate) {
			float priceDatePrice = priceDate.getPrice();
			if (price > priceDatePrice) {
				return 1;
			} else if (price < priceDatePrice) {
				return -1;
			} else {
				return 0;
			}
		}

		public float getPrice() {
			return price;
		}

		public void setPrice(float price) {
			this.price = price;
		}

		public int getMonth() {
			return month;
		}

		public void setMonth(int month) {
			this.month = month;
		}
	}
	
	/**
	 * Thread to read input file and do sanity check on it
	 * 
	 * @author Adib
	 */
	class Read extends Thread {
		final String inputFile;
		final Map<String, List<PriceDate>> flights;
		
		public Read(String inputFile) {
			this.inputFile = inputFile;
			flights = new HashMap<String, List<PriceDate>>();
		}
		
		@Override
		public void run() {
			readFile(inputFile);
		}
		
		/**
		 * Read input file and do sanity check on each record,
		 * also update mean and median value
		 * 
		 * @param inputFile The input OTP relative data-set location
		 */
		public void readFile(String inputFile) {
			try (
				
				InputStream gzipStream = new GZIPInputStream(new FileInputStream(inputFile));
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gzipStream));
				
			) {
				String currentLine;
				
				while ((currentLine = bufferedReader.readLine()) != null) {
					
					String[] row = parse(currentLine, 110); 
					if (row != null && sanityTest(row) && checkYear(row)) {
						updateFlights(row);
					}
				}
				
				synchronized (ThreadedAnalysis.flights) {
					updateFlights();
				}
				
			} catch (Exception exception) {
				
				System.out.println("Failed to read: " + inputFile);
				exception.printStackTrace();
				
			}
			
		}
		
		/**
		 * Add given row to flights map
		 * 
		 * @param row The row to add
		 */
		private void updateFlights(String row[]) {
			String carrierCode = row[8];
			int month = (int) Float.parseFloat(row[2]);
			float price = Float.parseFloat(row[109]);
			PriceDate priceDate = new PriceDate(price, month);
			List<PriceDate> flightData = flights.get(carrierCode);
			
			if (flightData == null) {
				List<PriceDate> priceDates = new ArrayList<PriceDate>();
				priceDates.add(priceDate);
				flights.put(carrierCode, priceDates);
			} else {
				flightData.add(priceDate);
			}	
		}
		
		/**
		 * Add the local flights map to global synchronized map
		 */
		private void updateFlights() {
			for (Map.Entry<String, List<PriceDate>> entry : flights.entrySet()) {
				String carrierCode = entry.getKey();
				List<PriceDate> flightDataLocal = entry.getValue();
				List<PriceDate> flightDataGlobal = ThreadedAnalysis.flights.get(carrierCode);
				
				if (flightDataGlobal == null) {
					ThreadedAnalysis.flights.put(carrierCode, flightDataLocal);
				} else {
					flightDataGlobal.addAll(flightDataLocal);
				}
			}
		}
		
		/**
		 * Convert time in hhmm format to minutes since day started
		 * 
		 * @param time The time in hhmm format
		 * @return Minutes minutes since day started
		 * @throws NumberFormatException
		 */
		private int timeToMinute(String time) throws NumberFormatException {
			if (time == null || time.length() == 0) {
				throw new NumberFormatException();
			}
			
			int hhmm = (int) Float.parseFloat(time);
			int hour = 0;
			int minute = 0;
			
			if (hhmm < 100) {
				minute = hhmm;
			} else {
				hour = hhmm / 100;
				minute = hhmm % 100; 
			}
			
			return hour * 60 + minute;
		}
		
		/**
		 * Check whether the given record passes the sanitary test
		 * 
		 * @param row Record of flight OTP data
		 * @return true iff it passes sanity test. False, otherwise
		 */
		private boolean sanityTest(String[] row) {
			try {
				
				// hh:mm format
				int CRSArrTime = timeToMinute(row[40]);
				int CRSDepTime = timeToMinute(row[29]);
				
				// Check for zero value
				if (CRSArrTime == 0 || CRSDepTime == 0) {
					return false;
				}
				
				// minutes format
				int CRSElapsedTime = (int) Float.parseFloat(row[50]);
				int timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;
				
				// Check for modulo zero
				if (timeZone % 60 != 0) {
					return false;
				}
				
				int originAirportId = (int) Float.parseFloat(row[11]);
				int destAirportId = (int) Float.parseFloat(row[20]);
				int originAirportSeqId = (int) Float.parseFloat(row[12]);
				int destAirportSeqId = (int) Float.parseFloat(row[21]);
				int originCityMarketId = (int) Float.parseFloat(row[13]);
				int destCityMarketId = (int) Float.parseFloat(row[22]);
				int originStateFips = (int) Float.parseFloat(row[17]);
				int destStateFips = (int) Float.parseFloat(row[26]);
				int originWac = (int) Float.parseFloat(row[19]);
				int destWac = (int) Float.parseFloat(row[28]);
				
				// Check for Ids greater than zero
				if (originAirportId <= 0 || destAirportId <= 0 || originAirportSeqId <= 0 || 
						destAirportSeqId <= 0 || originCityMarketId <= 0 || destCityMarketId <= 0 || 
						originStateFips <= 0 || destStateFips <= 0 || originWac <= 0 || destWac <= 0) {
					return false;
				}
				
				// Check for non-empty condition
				if (row[14].isEmpty() || row[23].isEmpty() || row[15].isEmpty() || row[24].isEmpty() ||
						row[16].isEmpty() || row[25].isEmpty() || row[18].isEmpty() || row[27].isEmpty()) {
					return false;
				}
				
				// For flights that are not cancelled
				int cancelled = (int) Float.parseFloat(row[47]);
				if (cancelled != 1) {
					int arrTime = timeToMinute(row[41]);
					int depTime = timeToMinute(row[30]);
					int actualElapsedTime = (int) Float.parseFloat(row[51]);
					
					// Check for zero value
					int time = arrTime - depTime - actualElapsedTime - timeZone;
					if (time != 0 && time % 1440 != 0) {
						return false;
					}
					
					int arrDelay = (int) Float.parseFloat(row[42]);
					int arrDelayMinutes = (int) Float.parseFloat(row[43]);
					if (arrDelay > 0 && arrDelay != arrDelayMinutes) {
						return false;
					} else if (arrDelay < 0 && arrDelayMinutes != 0) {
						return false;
					}
					
					if (arrDelayMinutes >= 15 && ((int) Float.parseFloat(row[44])) != 1) {
						return false;
					}
				}
				
			} catch (NumberFormatException exception) {
				
				return false;
				
			}
			
			return true;
		}
		
		/**
		 * Checks if the flight is on January 2015
		 * 
		 * @param row The record for a flight
		 * @return true if the flight is on given date, false otherwise
		 */
		private boolean checkYear(String row[]) {
			int year = (int) Float.parseFloat(row[0]);
			int quarter = (int) Float.parseFloat(row[1]);
			int month = (int) Float.parseFloat(row[2]);
			
			if (year == 2015 && quarter == 1 && month == 1) {
				return true;
			}
			
			return false;
		}
		
		/**
		 * Parse a CSV record given as string
		 * - Remove any quotes from record
		 * - Splits on each new column
		 * 
		 * @param record The record to parse 
		 * @return Array containing those records, null if couldn't parse
		 */
		private String[] parse(String record, int column) {
			
			String[] ans = new String[column];
			int col = 0;
			int len = record.length();
			StringBuilder builder = new StringBuilder();
			char[] arr = record.toCharArray();
			
			for (int i = 0; i < len; i++) {
				char ch = arr[i];
				
				// Remove quotes 
				if (ch == '\"') {
					continue;
				}
				
				if (ch != ',') {
					// No Split condition
					builder.append(ch);
					
					if (i == len - 1) {
						ans[col] = builder.toString();
					}
				} else if (i + 1 < len && arr[i + 1] == ' ') {
					// Ignore condition
					builder.append(ch);
				} else {
					// Split condition
					if (i == len - 1) {
						col = 0;
						break;
					}
					
					ans[col] = builder.toString();
					builder.setLength(0);
					col++;
				}
			}
			
			if (col < column - 1) {
				return null;
			}
			
			return ans;
		}
	}
	
	/**
	 * Calculate Mean value for each top carrier
	 * 
	 * @param top Number of top airlines to retrieve
	 * @param printStream Output to print to
	 */
	private void printMean(int top, PrintStream printStream) {
		
		List<Map.Entry<String, List<PriceDate>>> meanList = 
				new ArrayList<Map.Entry<String, List<PriceDate>>>(flights.entrySet());
		
		Collections.sort(meanList, new Comparator<Map.Entry<String, List<PriceDate>>>() {

			@Override
			public int compare(Entry<String, List<PriceDate>> o1, Entry<String, List<PriceDate>> o2) {
				float size1 = o1.getValue().size();
				float size2 = o2.getValue().size();
				
				if (size1 > size2) {
					return -1;
				} else if (size1 < size2) {
					return 1;
				} else {
					return 0;
				}
			}
			
		});
		
		for (int i = 0; i < meanList.size() && i < top; i++) {
			Map.Entry<String, List<PriceDate>> entry = meanList.get(i);
			String carrierCode = entry.getKey();
			List<PriceDate> priceDates = entry.getValue();
			
			Map<Integer, List<Float>> monthMap = new HashMap<Integer, List<Float>>();
			for (PriceDate priceDate : priceDates) {
				int month = priceDate.getMonth();
				float price = priceDate.getPrice();
				List<Float> prices = monthMap.get(month);
				
				if (prices == null) {
					List<Float> priceList = new ArrayList<Float>();
					priceList.add(price);
					monthMap.put(month, priceList);
				} else {
					prices.add(price);
				}
			}
			
			for (Map.Entry<Integer, List<Float>> entryMonth : monthMap.entrySet()) {
				int month = entryMonth.getKey();
				List<Float> prices = entryMonth.getValue();
				int count = prices.size();
				float sumPrice = 0;
				
				for (float price : prices) {
					sumPrice += price;
				}
				
				printStream.println(month + " " + carrierCode + " " + sumPrice / (float) count);
			}
		}
	}
	
	/**
	 * Calculate Median value for each top carrier
	 * 
	 * @param top Number of top airlines to retrieve
	 * @param printStream Output to print to
	 */
	private void printMedian(int top, PrintStream printStream) {
		
		List<Map.Entry<String, List<PriceDate>>> medianList = 
				new ArrayList<Map.Entry<String, List<PriceDate>>>(flights.entrySet());
		
		Collections.sort(medianList, new Comparator<Map.Entry<String, List<PriceDate>>>() {

			@Override
			public int compare(Entry<String, List<PriceDate>> o1, Entry<String, List<PriceDate>> o2) {
				float size1 = o1.getValue().size();
				float size2 = o2.getValue().size();
				
				if (size1 > size2) {
					return -1;
				} else if (size1 < size2) {
					return 1;
				} else {
					return 0;
				}
			}
			
		});
		
		for (int i = 0; i < medianList.size() && i < top; i++) {
			Map.Entry<String, List<PriceDate>> entry = medianList.get(i);
			String carrierCode = entry.getKey();
			List<PriceDate> priceDates = entry.getValue();
			
			Map<Integer, List<Float>> monthMap = new HashMap<Integer, List<Float>>();
			for (PriceDate priceDate : priceDates) {
				int month = priceDate.getMonth();
				float price = priceDate.getPrice();
				List<Float> prices = monthMap.get(month);
				
				if (prices == null) {
					List<Float> priceList = new ArrayList<Float>();
					priceList.add(price);
					monthMap.put(month, priceList);
				} else {
					prices.add(price);
				}
			}
			
			for (Map.Entry<Integer, List<Float>> entryMonth : monthMap.entrySet()) {
				int month = entryMonth.getKey();
				List<Float> prices = entryMonth.getValue();
				int count = prices.size();
				Collections.sort(prices);
				float medianPrice;
				
				if (count % 2 == 0) {
					medianPrice = (prices.get(count / 2) + prices.get(count / 2 - 1)) / 2;
				} else {
					medianPrice = prices.get(count / 2 - 1);
				}
				
				printStream.println(month + " " + carrierCode + " " + medianPrice);
			}
		}
	}
	
	/**
	 * Calculate Fast Median value for each top carrier
	 * 
	 * @param top Number of top airlines to retrieve
	 * @param printStream Output to print to
	 */
	private void printFastMedian(int top, PrintStream printStream) {
		List<Map.Entry<String, List<PriceDate>>> medianList = 
				new ArrayList<Map.Entry<String, List<PriceDate>>>(flights.entrySet());
		
		Collections.sort(medianList, new Comparator<Map.Entry<String, List<PriceDate>>>() {

			@Override
			public int compare(Entry<String, List<PriceDate>> o1, Entry<String, List<PriceDate>> o2) {
				float size1 = o1.getValue().size();
				float size2 = o2.getValue().size();
				
				if (size1 > size2) {
					return -1;
				} else if (size1 < size2) {
					return 1;
				} else {
					return 0;
				}
			}
			
		});
		
		for (int i = 0; i < medianList.size() && i < top; i++) {
			Map.Entry<String, List<PriceDate>> entry = medianList.get(i);
			String carrierCode = entry.getKey();
			List<PriceDate> priceDates = entry.getValue();
			
			Map<Integer, List<Float>> monthMap = new HashMap<Integer, List<Float>>();
			for (PriceDate priceDate : priceDates) {
				int month = priceDate.getMonth();
				float price = priceDate.getPrice();
				List<Float> prices = monthMap.get(month);
				
				if (prices == null) {
					List<Float> priceList = new ArrayList<Float>();
					priceList.add(price);
					monthMap.put(month, priceList);
				} else {
					prices.add(price);
				}
			}
			
			for (Map.Entry<Integer, List<Float>> entryMonth : monthMap.entrySet()) {
				int month = entryMonth.getKey();
				List<Float> prices = entryMonth.getValue();
				int count = prices.size();
				float medianPrice;
				if (count % 2 == 0) {
					medianPrice = (kthLargest(prices, 0, count, count / 2) + kthLargest(prices, 0, count, count / 2 - 1)) / 2;
				} else {
					medianPrice = kthLargest(prices, 0, count, count / 2 - 1);
				}
				
				printStream.println(month + " " + carrierCode + " " + medianPrice);
			}
		}
	}
	
	private int partition(List<Float> arr, int low, int high) {
		float pivot = arr.get(low);
		int left = low;

		for (int i = low + 1; i < high; i++) {
			if (arr.get(i) < pivot) {
				left++;
				float temp = arr.get(i);
				arr.set(i, arr.get(left));
				arr.set(left, temp);
			}
		}

		return left;
	}
	
	private float kthLargest(List<Float> arr, int low, int high, int k) {
		if (low < high) {
			int pivotLoc = partition(arr, low, high);
			float temp = arr.get(pivotLoc);
			arr.set(pivotLoc, arr.get(low));
			arr.set(low, temp);
			if (pivotLoc == k - 1) {
				return arr.get(pivotLoc);
			} else if (pivotLoc > k - 1) {
				return kthLargest(arr, low, pivotLoc, k);
			} else {
				return kthLargest(arr, pivotLoc + 1, high, k);
			}
		}

		return -1;
	}
	
	public static void main(String[] args) throws Exception {
		
		if (args.length < 2 || args.length > 3) {
			System.out.println("Usage (Sequential): <java> <mode> -input=DIR");
			System.out.println("Usage (Parallel): <java> -p <mode> -input=DIR");
			return;
		}
		
		long startTime = System.currentTimeMillis();
		
		int argsLen = args.length;
		int mode = Integer.parseInt(args[argsLen - 2]);
		ThreadedAnalysis threadedAnalysis = new ThreadedAnalysis();
		String dirPath = args[argsLen - 1].substring(7);
		File folder = new File(dirPath);
		File[] files = folder.listFiles();
		
		// Check for ending / or \
		char end = dirPath.charAt(dirPath.length() - 1);
		if (end == '/' || end == '\\') {
			dirPath = dirPath.substring(0, dirPath.length() - 1);
		}
		
		if (argsLen == 2) {
			
			for (int i = 0; i < files.length; i++) {
				Read read = threadedAnalysis.new Read(dirPath + "/" + files[i].getName());
				read.run();
			}
			
		} else {
			
			Thread[] readThreads = new Read[files.length];
			
			for (int i = 0; i < files.length; i++) {
				readThreads[i] = threadedAnalysis.new Read(dirPath + "/" + files[i].getName());
				readThreads[i].start();
			}
			
			for (Thread read : readThreads) {
				read.join();
			}
			
		}
		
		if (mode == ThreadedAnalysis.MODE_MEAN) {
			threadedAnalysis.printMean(10, System.out);
		} else if (mode == ThreadedAnalysis.MODE_MEDIAN) {
			threadedAnalysis.printMedian(10, System.out);
		} else if (mode == ThreadedAnalysis.MODE_FAST_MEDIAN) {
			threadedAnalysis.printFastMedian(10, System.out);
		}
		
		long endTime = System.currentTimeMillis();
		System.out.println((endTime - startTime) / 1000);
	}

}
