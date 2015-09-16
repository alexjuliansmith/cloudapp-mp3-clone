
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class FileReaderSpout implements IRichSpout {
	
  public static final String FILE_KEY = "file";
  
  private SpoutOutputCollector _collector;
  private TopologyContext context;
  
  private Iterator<String>  lines;

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

     /*
    ---------------------------------------------
    Task: initialize the file reader */
	String file = conf.get(FILE_KEY).toString();

    try {
    	lines = Files.readAllLines(Paths.get(file), Charset.defaultCharset()).iterator();
	} catch (IOException e) {
		System.err.println("Couldn't find: " + file);
		e.printStackTrace();
	}
   /* ------------------------------------------------- */

    this.context = context;
    this._collector = collector;
  }

  @Override
  public void nextTuple() {

     /*
    ---------------------------------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop*/
	  
	 if (lines.hasNext()) {
		 String next = lines.next().trim();
		 if (next.length() > 0) _collector.emit(new Values(next));  // only emit if the line is not blank
	 } else {
		 Utils.sleep(1000); // prevent busy loop when spout is exhausted 
	 }

   /* ------------------------------------------------- */


  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
   /*
    ---------------------------------------------
    Task: close the file
    // nothing to do file open and closed in open();

    ------------------------------------------------- */

  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
