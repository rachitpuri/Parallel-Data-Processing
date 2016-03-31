import java.io.BufferedReader;
import java.io.File;
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
 * Given a set of airlines data 
 * Performs Sanity Check, Mean and Median for all airlines.
 * @author rachit
 *
 */

public class Assignment01 {
	
	// Constants
	public static Integer success = 0;
	public static Integer failure = 0;
	public static HashMap<String, ArrayList<Float>> map = new HashMap<String, ArrayList<Float>>();
	public static HashMap<String, Float> solution = new HashMap<String, Float>();
	public static HashMap<String, Float> sol_median = new HashMap<String, Float>();
	
	/*
	 * This method is used to sort the given Hashmap based on value.
	 * @param unsorted : This is the parameter passed to sortedMap
	 * @return HashMap : returns the sorted hashmap
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
    
	/*
	 * This method parses set of files using multithreading.
	 * @param path : This is the path of the root folder containing
	 *               set of files.              
	 */
	public static void parallelExecution(String path) {
		File folder = new File(path);
		File[] listoffiles = folder.listFiles();
		MyThread t[] = new MyThread[listoffiles.length];
		
		for (int i=0; i<listoffiles.length; i++) {
			t[i] = new MyThread(path, listoffiles[i].getName());
			t[i].start();
		}
		
		for (int i=0; i<listoffiles.length; i++) {
			try {
				t[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		System.out.println("valid : " +success);
		System.out.println("Invalid : " +failure);
		
		for (Map.Entry<String, ArrayList<Float>> entry : map.entrySet()) {
			ArrayList<Float> list = entry.getValue();
			Collections.sort(list);
			if(list.size()%2 != 0) 
				sol_median.put(entry.getKey(), list.get(list.size()/2)); 
	        else
	        	sol_median.put(entry.getKey(), (list.get(list.size()/2) + list.get(list.size()/2 - 1))/2);
			float sum = 0;
			
			int size = entry.getValue().size();
			for (int i=0; i<size; i++) {
				sum += entry.getValue().get(i);
			}
			solution.put(entry.getKey(), sum/size);
		}
		
		HashMap<String, Float> result = sortedMap(solution);
		for (Map.Entry<String, Float> entry : result.entrySet()) {
			System.out.println(entry.getKey()+" "+entry.getValue() + " " +sol_median.get(entry.getKey()));
		}
	}
	
	/*
	 * This method parses the set of files sequentially
	 * @param path : This is the path of folder containing the
	 *               list of files
	 */
	public static void SequentialExecution(String path) {
		File folder = new File(path);
		File[] listoffiles = folder.listFiles();
		String content;
		MyThread ob = new MyThread();
		for (int i=0; i < listoffiles.length; i++) {
			GZIPInputStream gzip = null;
			try {
				//System.out.println("file :" +listoffiles[i]);
				gzip = new GZIPInputStream(new FileInputStream(listoffiles[i]));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
			try {
				while ((content = br.readLine()) != null){
					// perform sanity test
					content = content.replaceAll("\"", "");
					String formatedData = content.replaceAll(", ", ":");
					String[] data = formatedData.split(",");
					if (data.length != 110) {
						failure++;
					} else {
						if (ob.validData(data)) {
							success++;
						} else {
							failure++;
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		System.out.println("valid : " +success);
		System.out.println("Invalid : " +failure);
		
		for (Map.Entry<String, ArrayList<Float>> entry : MyThread.map_t.entrySet()) {
			ArrayList<Float> list = entry.getValue();
			Collections.sort(list);
			if(list.size()%2 != 0) 
				sol_median.put(entry.getKey(), list.get(list.size()/2)); 
	        else
	        	sol_median.put(entry.getKey(), (list.get(list.size()/2) + list.get(list.size()/2 - 1))/2);
			float sum = 0;
			
			int size = entry.getValue().size();
			for (int i=0; i<size; i++) {
				sum += entry.getValue().get(i);
			}
			solution.put(entry.getKey(), sum/size);
		}
		
		HashMap<String, Float> result = sortedMap(solution);
		for (Map.Entry<String, Float> entry : result.entrySet()) {
			System.out.println(entry.getKey()+" "+entry.getValue() + " " +sol_median.get(entry.getKey()));
		}
	}
	
	public static void main(String args[]) {
		
		if (args.length < 1) {
			System.out.println("Enter proper arguments:\n 1. For Parallel : -p -input=C:/Users/abc/ \n 2. For Sequential : -input=C:/Users/abc/");
			return;
		}
		long start = System.currentTimeMillis();
		String arg2, path;
		
		if (args.length == 2) {
			arg2 = args[1];
			String[] temp = arg2.split("=");
			path = temp[1];
			parallelExecution(path);
		} else {
			arg2 = args[0];
			String[] temp = arg2.split("=");
			path = temp[1];
			SequentialExecution(path);
		}
		

		long end = System.currentTimeMillis();
		System.out.println("Time :" +(end-start) +" ms");
		}
}

/**
 * Thread Class that parses the given file and returns when done 
 * @author rachit
 *
 */
class MyThread extends Thread {
	
	public String filename;
	public String dir;
	public int valid;
	public int invalid;
	public static HashMap<String, ArrayList<Float>> map_t = new HashMap<String, ArrayList<Float>>();
	
	/*
	 * Constructor
	 */
	public MyThread() {
		
	}
	
	/*
	 * Thread Constructor
	 * @param path : This is the path to the folder
	 * @param file : This is the file inside the folder
	 */
	public MyThread(String path, String file) {
		filename = file;
		dir = path;
		valid = 0;
		invalid = 0;
	}

	/*
	 * This method converts the given time into minutes
	 * @param time : Time passed in hhmm (1020)
	 * @return int : Time returned in minutes (10*60 + 20)
	 */
	public int timeToMinute (int time){
		if (time<100){
			return time;
		}
		else {
			int hour = time/100;
			int min = time %100;
			return hour*60 + min;
		}
		
	}
	
	/*
	 * This method performs the sanity check.
	 * @param data : Row passed 
	 * @return boolean : True, if it passes the sanity check
	 */
	public boolean validData(String[] data) {
		boolean validData = true;
		int year, quarter, month;
		try {
			year = Integer.valueOf(data[0]);
			quarter = Integer.valueOf(data[1]);
			month = Integer.valueOf(data[2]);
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
		if ((year == 2015) && (quarter == 1) && (month == 1)) {
			try {
				String carrier = data[8];
				float price = Float.valueOf(data[109]);
				
				if (map_t.containsKey(carrier)) {
					ArrayList<Float> list = map_t.get(carrier);
					list.add(price);
					map_t.put(carrier, list);
				} else {
					ArrayList<Float> list = new ArrayList<Float>();
					list.add(price);
					map_t.put(carrier, list);
				}		
			} catch (NumberFormatException e) {
			}
		}			
		return validData;
	}
	
	/*
	 * This method parses the String and split it into String[]
	 * @param s : String passed to method
	 * @return String[] : split the string into array based on ','
	 */
	public String[] parseContent(String s) {
		
		int column = 0;
		int len = s.length();
		String[] data = new String[110];
		char[] ch = s.toCharArray();
		StringBuilder sb = new StringBuilder();
		for (int i=0; i < len; i++) {
			if (ch[i] == '\"') {
				continue;
			}
			if (i == len - 1) {
				data[column] = sb.toString();
			}
			
			if ((ch[i] != ',') || ((ch[i] == ',') && (i+1 < len) && (ch[i+1] == ' '))) {
				sb.append(ch[i]);
			} else {
				if (i == len - 1) {
					column = 0;
				}	
				data[column] = sb.toString();
				sb.setLength(0);
				column++;
			}
		}
		if (column < 109) {
			return null;
		}
		return data;
	}
	
	@Override
	public void run() {
		GZIPInputStream gzip = null;
		//System.out.println("filename : " +filename);
		try {
			gzip = new GZIPInputStream(new FileInputStream(dir +"\\" +filename));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
		String content = null;
		try {
			while ((content = br.readLine()) != null) {
				String[] data = parseContent(content);
				if (data == null) {
					invalid++;
				} else {
					if (validData(data)) {
						valid++;
					} else {
						invalid++;
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		synchronized (Assignment01.success) {
			Assignment01.success += valid;
		}
		
		synchronized (Assignment01.failure) {
			Assignment01.failure += invalid;
		}
		
		synchronized (Assignment01.map) {
			Assignment01.map.putAll(map_t);
		}
	}
}
