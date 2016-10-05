import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
public class InvertedIndexReducer extends Reducer<Text, Text,Text,Text>{

	public InvertedIndexReducer() {
		// TODO Auto-generated constructor stub
	}
     @Override
     public void reduce(final Text key, final Iterable<Text> values, final Context context) 
    		 throws IOException, InterruptedException{
    	 //<keyword, <doc1, doc1,doc1,doc2,doc2...>>
    	 //<keyword,doc1\tdoc2>
    	 String lastBook = null;
    	 StringBuilder sb = new StringBuilder();
    	 int count = 0;
    	 int threshold = 100;
    	 for (Text value : values){
    		 if (lastBook != null &&value.toString().trim().equals(lastBook)){
    			 count++;
    			 continue;
    		 }
    		 if (lastBook != null && count <threshold){
    			 count = 1;
    			 lastBook =value.toString().trim();
    			 continue;
    		 }
    		 sb.append(lastBook);
    		 sb.append("\t");
    		 
    		 count = 1;
    		 lastBook = value.toString().trim();
    	 }
    	 if (threshold < count){
    		 sb.append(lastBook);
    	 }
         if (!sb.toString().trim().equals("")){
        	 context.write(key, new Text(sb.toString()));
         }
     }
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
