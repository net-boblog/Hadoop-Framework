package com.taomee.bigdata.driver;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.*;
import java.util.*;

public class SimpleJobDriver extends Configured
        implements Tool
{
    private static int printUsage()
    {
        System.out.println("SimpleJobDriver " +
                " -inFormat <input format class> " +
                " -outFormat <output format class> " +
                " -outKey <output key class> " +
                " -outValue <output value class> " +
                " -jobName <job name> " +
                " -mapperClass <mapper class> " +
                " -reduceClass <reducer class> " +
                " -gameInfo <game info> " +
                " [-combinerClass <combiner class>] " +
                " [-addMos <name>,<output format>,<output key class>,<output value class>] " +
                " [-setKeyComparator <key comparator class>] " +
                " [-setPartitioner <partitioner class> ] " +
                " [-setGroupComparator <group comparator class > ] " +
                " -input <input path> " +
                " -output <output path> ");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public int run(String[] args) throws IOException
    {
        JobConf jobConf = new JobConf(this.getConf(), this.getClass());

        Class<? extends InputFormat> inputFormatClass =
            jobConf.getInputFormat().getClass();
        Class<? extends OutputFormat> outputFormatClass =
            jobConf.getOutputFormat().getClass();
        Class<? extends WritableComparable> outputKeyClass =
            LongWritable.class;
        Class<? extends Writable> outputValueClass =
            Text.class;
        Class<? extends Mapper> mapperClass =
            IdentityMapper.class;
        Class<? extends Reducer> reducerClass =
            IdentityReducer.class;
        Class<? extends Reducer> combinerClass = null;
        List<String> mosNames = new ArrayList<String>();
        List<Class<? extends OutputFormat>> mosOutputFormats =
                new ArrayList<Class<? extends OutputFormat>>();
        List<Class<? extends WritableComparable>> mosOutputKeyClasses =
                new ArrayList<Class<? extends WritableComparable>>();
        List<Class<? extends Writable>> mosOutputValueClasses =
                new ArrayList<Class<? extends Writable>>();
        Class<? extends RawComparator> keyComparatorClass = null;
        Class<? extends Partitioner> partitionerClass = null;
        Class<? extends RawComparator> groupComparatorClass = null;

        boolean deleteExistOutput = false;
        Path inputPath = null;
        Path outputPath = null;

        List<String> otherArgs = new ArrayList<String>();
        String jobName = "";
        int i = 0;

		List<String> gameId = new ArrayList<String>();

        try {
            for (i = 0; i < args.length; i++) {

                if (args[i].equals("-inFormat")) {
                    inputFormatClass =
                        Class.forName(args[++i]).asSubclass(InputFormat.class);
                } else if (args[i].equals("-outFormat")) {
                    outputFormatClass =
                        Class.forName(args[++i]).asSubclass(OutputFormat.class);
                } else if (args[i].equals("-outKey")) {
                    outputKeyClass =
                        Class.forName(args[++i]).asSubclass(WritableComparable.class);
                } else if (args[i].equals("-outValue")) {
                    outputValueClass =
                        Class.forName(args[++i]).asSubclass(Writable.class);
                } else if (args[i].equals("-jobName")) {
                    jobName = args[++i];
                } else if (args[i].equals("-mapperClass")) {
                    mapperClass = Class.forName(args[++i]).asSubclass(Mapper.class);
                } else if (args[i].equals("-reducerClass")) {
                    reducerClass = Class.forName(args[++i]).asSubclass(Reducer.class);
                } else if (args[i].equals("-combinerClass")) {
                    combinerClass = Class.forName(args[++i]).asSubclass(Reducer.class);
                } else if (args[i].equals("-input")) {
                    inputPath = new Path(args[++i]);
                } else if (args[i].equals("-output")) {
                    outputPath = new Path(args[++i]);
                } else if (args[i].equals("-gameInfo")) {
					String gameinfo_temp = args[++i];
					//transmit gameinfo to jobConf
					jobConf.set("GameInfo",gameinfo_temp);
					String[] gameinfo = gameinfo_temp.split(",");
					if (gameinfo.length < 1) {
						throw new IllegalArgumentException("invalid add gameInfo");
					}
					for(int j = 0; j < gameinfo.length; j++) {
						gameId.add(gameinfo[j]);
					}
				}else if (args[i].equals("-addMos")) {

                    String val = args[++i];
                    System.err.println("val " + val);
                    String[] valItems = val.split(",");
                    if (valItems.length < 4) {
                        throw new IllegalArgumentException("invalid add mos format");
                    }
					for (int j = 0; j < gameId.size(); j++) {
						mosNames.add(valItems[0] + "G" + gameId.get(j));
						mosOutputFormats.add(
								Class.forName(valItems[1]).asSubclass(OutputFormat.class));
						mosOutputKeyClasses.add(
								Class.forName(valItems[2]).asSubclass(WritableComparable.class));
						mosOutputValueClasses.add(
								Class.forName(valItems[3]).asSubclass(Writable.class));
                    }
                    mosNames.add(valItems[0]);
                    mosOutputFormats.add(
                            Class.forName(valItems[1]).asSubclass(OutputFormat.class));
                    mosOutputKeyClasses.add(
                            Class.forName(valItems[2]).asSubclass(WritableComparable.class));
                    mosOutputValueClasses.add(
                            Class.forName(valItems[3]).asSubclass(Writable.class));

                } else if (args[i].equals("-setKeyComparator")) {
                    keyComparatorClass = Class.forName(args[++i]).asSubclass(
                            RawComparator.class);
                } else if (args[i].equals("-setPartitioner")) {
                    partitionerClass = Class.forName(args[++i]).asSubclass(
                            Partitioner.class);
                } else if (args[i].equals("-setGroupComparator")) {
                    groupComparatorClass = Class.forName(args[++i]).asSubclass(
                            RawComparator.class);
                } else {
                    otherArgs.add(args[i]);
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("missing param for " + args[i - 1]);
            return printUsage();
        } catch (ClassNotFoundException e) {
            System.out.println("class not found for " + args[i - 1]);
            return printUsage();
        }
		for (int j = 0; j < gameId.size(); j++) {
			mosNames.add("partG"+gameId.get(j));
			mosOutputFormats.add(outputFormatClass);
			mosOutputKeyClasses.add(outputKeyClass);
			mosOutputValueClasses.add(outputValueClass);
		}   

        if (inputPath == null) {
            System.out.println("no input path");
            return printUsage();
        }

        if (outputPath == null) {
            System.out.println("no output path");
            return printUsage();
        }

        FileSystem fs = FileSystem.get(this.getConf());

        System.out.println("input path = " + inputPath.toString());
        System.out.println("output path = " + outputPath.toString());

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        for (i = 0; i < mosNames.size(); i++) {
            System.err.println("add multi outputs '" + mosNames.get(i) + "'");
            MultipleOutputs.addNamedOutput(
                    jobConf, mosNames.get(i),
                    mosOutputFormats.get(i), mosOutputKeyClasses.get(i),
                    mosOutputValueClasses.get(i));
        }

        FileInputFormat.addInputPath(jobConf, inputPath);
        FileOutputFormat.setOutputPath(jobConf, outputPath);
        jobConf.setJobName(jobName);
        jobConf.setMapperClass(mapperClass);
        jobConf.setReducerClass(reducerClass);
        if (combinerClass != null) {
            System.out.println("set combiner class " + combinerClass.toString());
            jobConf.setCombinerClass(combinerClass);
        }
        jobConf.setInputFormat(inputFormatClass);
        jobConf.setOutputFormat(outputFormatClass);
        jobConf.setOutputKeyClass(outputKeyClass);
        jobConf.setOutputValueClass(outputValueClass);

        if (keyComparatorClass != null) {
            System.err.println("set key comparator " + keyComparatorClass);
            jobConf.setOutputKeyComparatorClass(keyComparatorClass);
        }

        if (partitionerClass != null) {
            System.err.println("set partitioner " + partitionerClass);
            jobConf.setPartitionerClass(partitionerClass);
        }

        if (groupComparatorClass != null) {
            System.err.println("set group comparator " + groupComparatorClass);
            jobConf.setOutputValueGroupingComparator(groupComparatorClass);
        }

        JobClient.runJob(jobConf);

        return 0;
    }

    public static void main(String[] args) throws Exception
    {
        int ret = ToolRunner.run(new SimpleJobDriver(), args);
        System.exit(ret);
    }
}
