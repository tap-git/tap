The Tap Framework
=================

Tap is a new framework for batch and streaming big data programming in Java. It's designed to make it easy to write programs using an OO-style, programming with POJOs. It comes with support for working with pipelines - determining file dependencies and running the phases needed to emit the corresponding results. The framework is still in alpha test, but we'd love you to try it and give us feedback. Thank you!

## Key concepts:

* a Pipe - defines a set of inter-related phases (i.e., Hadoop jobs) that produce final outputs. This is represented by an instance of the class Pipe.
* a Phase - defines a single map/reduce pair that transforms an input to an output. This will include a phase that joins multiple inputs to produce a single output. This is represented by an instance of the class Phase.

## Creating a Pipe

Tap uses a fluid/builder syntax much like jMock, in which you can apply multiple changes to a single object. You first need to create a pipe:

    Tap analysis = new Tap(getClass()).named("analysis");

You define the input and output pipes (files) that are important to you, e.g.,

    Pipe<LogRecord> input = Pipe<LogRecord>(o.input+"/"+timePath);
    Pipe<ReportRecord> output = Pipe.of(ReportRecord.class).at(o.output+"/"+timePath);

You also need to declare the final outputs that your pipeline needs to produce:

    analysis.produces(output);

Finally you define the phases (jobs) that produce outputs from their inputs:

    Phase analyze = new Phase().map(AnalysisMapper.class).reduce(AnalysisReducer.class)
      .groupBy("id,str").sortBy("count desc").reads(input).writes(output);

Mappers and reducers in Tap work on objects, both for input and output. A mapper looks like this:

    public static class AnalysisMapper extends BaseMapper<LogRecord, ReportRecord> {
      @Override
      public void map(LogRecord in, ReportRecord out, TapContext< ReportRecord> context) {
        out.id = in.id;
        for (String str : in.comments) {
          out.str = str;
          out.count = 1;
          context.write(out);
        }
      }
    }

A reducer looks like this:

    public static class AnalysisReducer extends BaseReducer<ReportRecord, ReportRecord> {
      @Override
      public void reduce(Iterable<ReportRecord> in, ReportRecord out, TapContext<ReportRecord> context) {
        ReportRecord out = null;
        for (ReportRecord rec : in) {
          if (out == null) {
            RecordUtils.copy(in, out);
          } else {
            out.count++;
          }
        }
        context.write(out);
      }
    }

The default file format for Tap is [Avro](http://avro.apache.org/), but it has support for text and JSON output and soon we'll add support for [Protobuf](http://code.google.com/p/protobuf/) and pluggable formats to enable support for a variety of input and output formats. Currently, you use Avro schemas to define your objects, then use the Avro code generation tool to create Java classes to represent them. As soon as Avro effectively supports reflection-based map/reduce we'll also support reflective access to your Java classes, letting them define the file schema instead.
 
