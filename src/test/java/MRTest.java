import combiner.LWCombiner;
import mapper.LWMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import reducer.LWReducer;

import java.io.IOException;
import java.util.*;

public class MRTest {
        MapDriver mapDriver;
        ReduceDriver<IntWritable, Text, IntWritable, Text> reduceDriver;
        ReduceDriver<IntWritable, Text, IntWritable, Text> combinerDriver;
        MapReduceDriver<LongWritable, Text, IntWritable, Text, IntWritable, Text> mapReduceDriver;

    @Before
    public void setUp() {
        LWMapper mapper = new LWMapper();
        LWCombiner combiner = new LWCombiner();
        LWReducer reducer = new LWReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        combinerDriver = ReduceDriver.newReduceDriver(combiner);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("dog cat"));
        mapDriver.withOutput(new IntWritable(-3), new Text("cat; dog; "));
        mapDriver.runTest();
        }

    @Test
    public void testCombiner() throws IOException {
        final  String cat="cat";
        final  String dog="dog";
        List<Text> values = new ArrayList<Text>();
        values.add(new Text(cat));
        values.add(new Text(dog));
        combinerDriver.withInput(new IntWritable(-3), values);
        combinerDriver.withOutput(new IntWritable(-3), new Text("cat; dog; "));
        combinerDriver.runTest();
    }


    @Test
    public void testReducer() throws IOException {
        final  String cat="cat";
        final  String dog="dog";
        List<Text> values = new ArrayList<Text>();
        values.add(new Text(cat));
        values.add(new Text(dog));
        reduceDriver.withInput(new IntWritable(-3), values);
        reduceDriver.withOutput(new IntWritable(3), new Text("cat; dog; "));
        reduceDriver.runTest();
        }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text("cat dog"));
        mapReduceDriver.withOutput(new IntWritable(3), new Text("cat; dog; "));
        mapReduceDriver.runTest();
        }
        }
