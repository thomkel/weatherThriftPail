package edu.uchicago.mpcs53013.weatherThriftPail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Collections ;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.PailStructure;
import com.backtype.hadoop.pail.SequenceFileFormat;

import edu.uchicago.mpcs53013.weatherThriftPail.PailSerialize.WeatherData;



public class PailPartitionedSerialize {
	public static class WeatherData {
		public int year;
		public int month;
		public int day;
		public double temperature;

		public WeatherData(int year, int month, int day, double temperature) {
			this.year = year;
			this.month = month;
			this.day = day;
			this.temperature = temperature;
		}
	} 
	public static class WeatherDataPailStructure implements PailStructure<WeatherData>{
		public Class getType() {
			return WeatherData.class;
		}
		public byte[] serialize(WeatherData weatherData) {
			ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			DataOutputStream dataOut = new DataOutputStream(byteOut);
//			byte[] userBytes = weatherData.userName.getBytes();
			try {
//				dataOut.writeInt(userBytes.length);
//				dataOut.write(userBytes);
//				dataOut.writeLong(login.loginUnixTime);
//				dataOut.close();
				dataOut.writeInt(weatherData.year);
				dataOut.writeInt(weatherData.month);
				dataOut.writeInt(weatherData.day);
				dataOut.writeDouble(weatherData.temperature);
				dataOut.close();
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
			return byteOut.toByteArray();
		}

		public WeatherData deserialize(byte[] serialized) {
			DataInputStream dataIn =
					new DataInputStream(new ByteArrayInputStream(serialized));
			try {
				int year = dataIn.readInt();
				int month = dataIn.readInt();
				int day = dataIn.readInt();
				double temperature = dataIn.readDouble();
				return new WeatherData(year, month, day, temperature);
//				byte[] userBytes = new byte[dataIn.readInt()];
//				dataIn.read(userBytes);
//				return new WeatherData(new String(userBytes), dataIn.readLong());
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}
		public List<String> getTarget(WeatherData object) {
			return Collections.EMPTY_LIST;
		}
		public boolean isValidTarget(String... dirs) {
			return true;
		}
	}
	public static class PartitionedWeatherDataPailStructure extends WeatherDataPailStructure {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		public List<String> getTarget(WeatherData object) {
			ArrayList<String> directoryPath = new ArrayList<String>();
//			Date date = new Date(object.loginUnixTime * 1000L);
			Date date = new Date(object.year, object.month, object.day);
			directoryPath.add(formatter.format(date));
			return directoryPath;
		}
		public boolean isValidTarget(String... strings) {
			if(strings.length > 2) return false;
			try {
				return (formatter.parse(strings[0]) != null);
			}
			catch(ParseException e) {
				return false;
			}
		}
	}
	
	static Pail<WeatherData> createUncompressedPail(FileSystem fs) throws IOException {
		return Pail.create(fs,
				"/tmp/weatherUncompressed",
				new PartitionedWeatherDataPailStructure());
		
	}
	
	static Pail<WeatherData> createCompressedPail(FileSystem fs) throws IOException {
		Map<String, Object> options = new HashMap<String, Object>();
		options.put(SequenceFileFormat.CODEC_ARG,
				SequenceFileFormat.CODEC_ARG_GZIP);
		options.put(SequenceFileFormat.TYPE_ARG,
				SequenceFileFormat.TYPE_ARG_BLOCK);
		WeatherDataPailStructure struct = new WeatherDataPailStructure();
		return Pail.create(fs, "/tmp/weatherCompressed",
				new PailSpec("SequenceFile", options, struct));
		
	}
	public static void writeWeatherData() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path(hadoopPrefix + "/conf/core-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		Pail<WeatherData> weatherDataPail = compress ? createCompressedPail(fs) : createUncompressedPail(fs);
//		TypedRecordOutputStream out = weatherDataPail.openWrite();
		
		
		// found code https://sites.google.com/site/hadoopandhive/home/how-to-read-all-files-in-a-directory-in-hdfs-using-hadoop-filesystem-api
		FileStatus[] status = fs.listStatus(new Path("/tmp/weatherData"));
//		System.out.print("test: ");
//		System.out.print(status);
        for (int i=0;i<status.length;i++){
        		TypedRecordOutputStream out = weatherDataPail.openWrite();
        	
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String line;
                line=br.readLine(); // skip first line of file-- header data
                line=br.readLine();
                while (line != null){
                	char[] charArray = line.toCharArray(); 
                	int year = Integer.parseInt(new String(Arrays.copyOfRange(charArray, 14, 18)));
                	int month = Integer.parseInt(new String(Arrays.copyOfRange(charArray, 18, 20)));
                	int day = Integer.parseInt(new String(Arrays.copyOfRange(charArray, 20, 22)));
                	double temperature = Double.parseDouble(new String(Arrays.copyOfRange(charArray, 24, 30)));
                	System.out.println(year + "-" + month + day + ", temp: " + temperature);
                	out.writeObject(new WeatherData(year, month, day, temperature));
                        
                	line=br.readLine();
                }
        		out.close();
        }
//		out.writeObject(new Login("alex", 1352679231));
//		out.writeObject(new Login("bob", 1352674216));
//		out.writeObject(new Login("charlie", 1267421658));
		
//		out.close();
	}
	public static void readWeatherData() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path(hadoopPrefix + "/conf/core-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		Pail<WeatherData> weatherDataPail = new Pail<WeatherData>(fs, "/tmp/weatherUncompressed");
		for(WeatherData l : weatherDataPail) {
			System.out.println(l.year + " " + l.month + " " + l.day + " " + l.temperature );
		}
	}
	static String hadoopPrefix = System.getenv("HADOOP_PREFIX");
	static boolean compress = false;
	public static void main(String[] args) {
		if(hadoopPrefix == null) {
			throw new RuntimeException("Please set HADOOP_PREFIX environment variable");
		}
		try {
			if(args[0].equals("s")) {
				writeWeatherData();
			} if (args[0].equals("c")) {
				compress = true;
				writeWeatherData();
			} else {
				readWeatherData();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
