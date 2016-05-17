package com.taomee.bigdata.driver;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.*;

public class MultipleInputsJobDriver extends Configured
    implements Tool
{
    private static int printUsage()
    {
        System.out.println("MultipleInputsJobDriver " +
                " -jobName <job name> " +
                " -inFormat <input format> " +
                " -outFormat <output format >" +
                " -reducerClass <reducer class> " +
                " -combinerClass <combiner class> " +
                " -outKey <output key class> " +
                " -outValue <output value class> " +
                " -gameInfo <game info> " +
                " -addInput <input path>,<mapper class> " +
                " [-addMos <name>,<output format>,<output key class>,<output value class>] " +
                " [-setKeyComparator <key comparator class>] " +
                " [-setPartitioner <partitioner class> ] " +
                " [-setGroupComparator <group comparator class > ] " +
                " -output <output path> ");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public int run(String[] args) throws IOException
    {
        JobConf jobConf = new JobConf(this.getConf(), this.getClass());
        List<Path> inputPaths = new ArrayList<Path>();
        List<Class<? extends Mapper>> inputMapClasses =
            new ArrayList<Class<? extends Mapper>>();
        Path outputPath = null;
        Class<? extends Reducer> reducerClass = null;
        Class<? extends Reducer> combinerClass = null;
        Class<? extends WritableComparable> outputKeyClass = Text.class;
        Class<? extends Writable> outputValueClass = Text.class;
        Class<? extends InputFormat> inputFormatClass =
            jobConf.getInputFormat().getClass();
        Class<? extends OutputFormat> outputFormatClass =
            jobConf.getOutputFormat().getClass();
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

        List<String> gameId = new ArrayList<String>();

        List<String> otherArgs = new ArrayList<String>();
        String jobName = "";
        int i = 0;

        for (i = 0; i < args.length; i++) {
            System.out.println("args " + i + " = " + args[i]);
        }

        try {
            for (i = 0; i < args.length; i++) {

                if (args[i].equals("-jobName")) {

                    jobName = args[++i];
                } else if (args[i].equals("-addInput")) {

                    String val = args[++i];
                    System.err.println("val " + val);
                    String[] valItems = val.split(",");
                    if (valItems.length < 2) {
                        throw new IllegalArgumentException("invalid add input format");
                    }
                    inputPaths.add(new Path(valItems[0]));
                    inputMapClasses.add(
                            Class.forName(valItems[1]).asSubclass(Mapper.class));
                    //System.out.println("path: " + valItems[0] + ", class: " + valItems[1]);
                } else if (args[i].equals("-output")) {

                    outputPath = new Path(args[++i]);
                } else if (args[i].equals("-reducerClass")) {

                    reducerClass = Class.forName(args[++i]).asSubclass(Reducer.class);
                } else if (args[i].equals("-outKey")) {

                    outputKeyClass =
                        Class.forName(args[++i]).asSubclass(WritableComparable.class);
                } else if (args[i].equals("-outValue")) {

                    outputValueClass =
                        Class.forName(args[++i]).asSubclass(Writable.class);
                } else if (args[i].equals("-inFormat")) {

                    inputFormatClass =
                        Class.forName(args[++i]).asSubclass(InputFormat.class);
                } else if (args[i].equals("-outFormat")) {

                    outputFormatClass =
                        Class.forName(args[++i]).asSubclass(OutputFormat.class);
                }else if (args[i].equals("-gameInfo")) {
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

                } else if (args[i].equals("-combinerClass")) {
                    combinerClass = Class.forName(args[++i]).asSubclass(Reducer.class);
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
        } catch (IllegalArgumentException e) {
            System.out.println("illegal arguments:" + e.toString());
            return printUsage();
        }
        for (int j = 0; j < gameId.size(); j++) {
            mosNames.add("partG"+gameId.get(j));
            mosOutputFormats.add(outputFormatClass);
            mosOutputKeyClasses.add(outputKeyClass);
            mosOutputValueClasses.add(outputValueClass);
        }   

        if (inputPaths.size() == 0) {
            System.out.println("no input paths");
            return printUsage();
        }

        if (reducerClass == null) {
            System.out.println("missing -reducerClass param");
            return printUsage();
        }

        if (outputPath == null) {
            System.out.println("missing -output param");
            return printUsage();
        }

        FileSystem fs = FileSystem.get(this.getConf());

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

        //jobConf.setInputFormat(inputFormatClass);
        jobConf.setOutputFormat(outputFormatClass);

        FileOutputFormat.setOutputPath(jobConf, outputPath);

        for (i = 0; i < inputPaths.size(); i++) {
            MultipleInputs.addInputPath(jobConf, inputPaths.get(i),
                    inputFormatClass, inputMapClasses.get(i));
        }

        jobConf.setJobName(jobName);
        jobConf.setReducerClass(reducerClass);
        if (combinerClass != null) {
            System.err.println("set combiner class " + combinerClass);
            jobConf.setCombinerClass(combinerClass);
        }
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
        int ret = ToolRunner.run(new MultipleInputsJobDriver(), args);
        System.exit(ret);
    }
}
