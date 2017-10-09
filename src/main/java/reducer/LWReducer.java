package reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class LWReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    private Set<String> set;

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        if (set == null) {

            set = new HashSet<String>();

            for (Text words : values) {
                for (String word : words.toString().split("; ")) {
                    set.add(word);
                }
            }

            StringBuilder sb = new StringBuilder();
            for (String words : set) {
                sb.append(words.toString()).append("; ");
            }

            sb.setLength(sb.length() - 1);

            context.write(new IntWritable(-1 * key.get()), new Text(sb.toString()));
        }
    }

}