import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * a bolt that finds the top n words.
 */
public class TopNFinderBolt extends BaseBasicBolt {
  private HashMap<String, Integer> currentTopWords = new HashMap<String, Integer>();
  private int N;

  private long intervalToReport = 20;
  private long lastReportTime = System.currentTimeMillis();
  
  private int smallestTopCount = 0;

  public TopNFinderBolt(int N) {
    this.N = N;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
 /*
    ----------------------TODO-----------------------
    Task: keep track of the top N words */
	  
	  int count = tuple.getIntegerByField(WordCountBolt.COUNT_FIELD);

	  if (count > smallestTopCount || currentTopWords.size() < N) { 
		  currentTopWords.put(tuple.getStringByField(WordCountBolt.WORD_FIELD), count);
		  if (currentTopWords.size() > N) {
			  int nextSmallest = Integer.MAX_VALUE;
			  for (Map.Entry<String, Integer> next : currentTopWords.entrySet()) {
				  int nextCount = next.getValue();
				  if (nextCount == smallestTopCount) {
					  currentTopWords.remove(next.getKey());  //TODO, small bug here: because it may remove several if they all share the same min.
				  } else {
					  if (nextCount < nextSmallest) nextSmallest = nextCount;
				  }
			  }
			  smallestTopCount = nextSmallest;
		  }
		  
	  }


    /* ------------------------------------------------- */


    //reports the top N words periodically
    if (System.currentTimeMillis() - lastReportTime >= intervalToReport) {
      collector.emit(new Values(printMap()));
      lastReportTime = System.currentTimeMillis();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

     declarer.declare(new Fields("top-N"));

  }

  public String printMap() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("top-words = [ ");
    for (String word : currentTopWords.keySet()) {
      stringBuilder.append("(" + word + " , " + currentTopWords.get(word) + ") , ");
    }
    int lastCommaIndex = stringBuilder.lastIndexOf(",");
    stringBuilder.deleteCharAt(lastCommaIndex + 1);
    stringBuilder.deleteCharAt(lastCommaIndex);
    stringBuilder.append("]");
    return stringBuilder.toString();

  }
}
