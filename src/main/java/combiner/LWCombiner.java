package combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class LWCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {

    private Set<String> set;
    private int length;

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        if (set == null) {
            length = 0;
            set = new HashSet<String>();

            for (Text words : values) {
                for (String word : words.toString().split("; ")) {
                    if (word.length() < length)
                        continue;

                    if (word.length() > length) {
                        set.clear();
                        length = word.length();
                    }
                    set.add(word);
                }
            }

            StringBuilder sb = new StringBuilder();
            for (String words : set) {
                sb.append(words.toString()).append("; ");
            }

            context.write(new IntWritable(key.get()), new Text(sb.toString()));
        }

    }

}