package mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class LWMapper extends Mapper<LongWritable, Text, IntWritable, Text> {


    private StringBuilder sb = new StringBuilder();
    private Set<String> set = new HashSet<String>();
    private int length;


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        length = 0;
        String[] words = value.toString().split("\\W");

        for (String word : words) {

            if (word.length() < length)
                continue;

            if (word.length() > length) {
                set.clear();
                length = word.length();
            }

            set.add(word);

        }

        if (length > 0) {
            sb.setLength(0);
            for (String longWord : set) {
                sb.append(longWord).append("; ");
            }
            context.write(new IntWritable(-1 * length), new Text(sb.toString()));

        }


    }
}
