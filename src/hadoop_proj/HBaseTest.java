package hadoop_proj;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseTest {
	
	public static void main(String[] args) {
		try {
			HBaseConfiguration config = new HBaseConfiguration();
		    HTable htable = new HTable(config, "member");
			if(htable != null){
				HTableDescriptor descriptor = htable.getTableDescriptor();
				System.out.println(descriptor.getNameAsString());
				HColumnDescriptor[] cols = descriptor.getColumnFamilies();
				for (int i = 0; i < cols.length; i++) {
					HColumnDescriptor desc = cols[i];
					String family = desc.getNameAsString();
					System.out.println(family);
					ResultScanner scanner = htable.getScanner(Bytes.toBytes(family));
					Iterator<Result> iterator = scanner.iterator();
					while(iterator.hasNext()){
						Result result = iterator.next();
						KeyValue[] values = result.raw(); 
						for (int j = 0; j < values.length; j++) {
							KeyValue value = values[j];
							System.out.println("\t"+new String(value.getColumn())+"\t"+new String(value.getValue()));
						}
					}
				}
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
