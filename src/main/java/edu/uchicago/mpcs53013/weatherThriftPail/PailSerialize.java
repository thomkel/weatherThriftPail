package edu.uchicago.mpcs53013.weatherThriftPail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Collections ;
import java.util.Map;
import java.lang.RuntimeException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;
import com.backtype.hadoop.pail.PailStructure;



public class PailSerialize {
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
		public Class<WeatherData> getType() {
			return WeatherData.class;
		}
		public byte[] serialize(WeatherData weatherData) {
			ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			DataOutputStream dataOut = new DataOutputStream(byteOut);
			//byte[] userBytes = login.userName.getBytes();
			try {
//				dataOut.writeInt(userBytes.length);
//				dataOut.write(userBytes);
//				dataOut.writeLong(login.loginUnixTime);
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
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void writeWeatherData() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path(hadoopPrefix + "/conf/core-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		Pail<WeatherData> weatherDataPail = Pail.create(fs,
				"/tmp/weather",
				new WeatherDataPailStructure());
		
		// found code https://sites.google.com/site/hadoopandhive/home/how-to-read-all-files-in-a-directory-in-hdfs-using-hadoop-filesystem-api
		FileStatus[] status = fs.listStatus(new Path("/tmp/weatherData"));
		System.out.print("test: ");
		System.out.print(status);
        for (int i=0;i<status.length;i++){
        		TypedRecordOutputStream out = weatherDataPail.openWrite();
        	
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String line;
                line=br.readLine(); // skip first line of file-- header data
                line=br.readLine();
                while (line != null){
//                        System.out.println(line);
                	char[] charArray = line.toCharArray(); 
                	int year = Integer.parseInt(new String(Arrays.copyOfRange(charArray, 14, 18)));
                	int month = Integer.parseInt(new String(Arrays.copyOfRange(charArray, 18, 20)));
                	int day = Integer.parseInt(new String(Arrays.copyOfRange(charArray, 20, 22)));
                	double temperature = Double.parseDouble(new String(Arrays.copyOfRange(charArray, 24, 30)));
                	out.writeObject(new WeatherData(year, month, day, temperature));
                        
                	line=br.readLine();
                }
        		out.close();
        }
		
		
//		TypedRecordOutputStream out = weatherDataPail.openWrite();
//		out.writeObject(new Login("alex", 1352679231));
//		out.writeObject(new Login("bob", 1352674216));
//		out.close();
	}
	public static void readWeatherData() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path(hadoopPrefix + "/conf/core-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		Pail<WeatherData> weatherDataPail = new Pail<WeatherData>(fs, "/tmp/weather");
		for(WeatherData l : weatherDataPail) {
			System.out.println(l.year + " " + l.month + " " + l.day + " " + l.temperature );
		}
	}
	static String hadoopPrefix = System.getenv("HADOOP_PREFIX");
	public static void main(String[] args) {
		if(hadoopPrefix == null) {
			throw new RuntimeException("Please set HADOOP_PREFIX environment variable");
		}
		try {
			if(args[0].equals("s")) {
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
