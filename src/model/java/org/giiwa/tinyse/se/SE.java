package org.giiwa.tinyse.se;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.giiwa.core.bean.Helper.W;
import org.giiwa.core.bean.TimeStamp;
import org.giiwa.core.bean.X;
import org.giiwa.core.task.Task;
import org.wltea.analyzer.lucene.IKAnalyzer;

/**
 * local memory SE
 * 
 * @author wujun
 *
 */
public class SE {

  /**
   * MAX results
   */
  public static final int             MAX_RESULTS = 10000;

  static Log                          log         = LogFactory.getLog(SE.class);
  static final String                 _TYPE       = "_type";
  static long                         FLAG        = System.currentTimeMillis();

  private static RAMDirectory         ram;
  private static IndexWriter          writer;
  private static IndexSearcher        searcher;

  private static Map<String, Counter> counter     = new HashMap<String, Counter>();

  /**
   * get the supports types
   * 
   * @return Set of types
   */
  public static Set<String> getTypes() {
    return searchables.keySet();
  }

  /**
   * inc error count
   * 
   * @param type
   * @return int of current error count
   */
  public static int error(String type) {
    Counter c = counter.get(type);
    return c == null ? 0 : c.error;
  }

  /**
   * get the index usage=timecost/count for a type
   * 
   * @param type
   * @return float of the usage
   */
  public static float index(String type) {
    Counter c = counter.get(type);
    return c == null ? 0 : c.indexcost / c.indextimes;
  }

  /**
   * get the search usage=timecost/count
   * 
   * @param type
   * @return float of the usage
   */
  public static float search(String type) {
    Counter c = counter.get(type);
    return c == null ? 0 : c.searchcost / c.searchtimes;
  }

  /**
   * get the max cost of search for a type
   * 
   * @param type
   * @return the time cost
   */
  public static long searchmax(String type) {
    Counter c = counter.get(type);
    return c == null ? 0 : c.searchmax;
  }

  /**
   * get the min cost o search for a type
   * 
   * @param type
   * @return the time cost
   */
  public static long searchmin(String type) {
    Counter c = counter.get(type);
    return c == null ? 0 : c.searchmin;
  }

  /**
   * count the documents that indexed
   * 
   * @param type
   * @return the number of the documents
   */
  public static int count(String type) {
    /**
     * avoid the searcher was changed by indexer
     */
    IndexSearcher searcher = SE.searcher;

    try {
      BooleanQuery b = new BooleanQuery(); // for quest
      b.add(new TermQuery(new Term(_TYPE, type)), Occur.MUST);
      TopDocs d = searcher.search(b, 1);
      return d.totalHits;
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
    return -1;

  }

  private static IndexerTask indexer;

  private static Analyzer    analyzer;

  /**
   * initialize the SE
   * 
   * @param conf
   *          the configuration
   */
  public synchronized static void init(Configuration conf) {
    if (ram != null) {
      return;
    }

    try {
      ram = new RAMDirectory();
      analyzer = new IKAnalyzer();
      writer = new IndexWriter(ram, new IndexWriterConfig(Version.LATEST, analyzer));
      writer.commit();

      IndexReader reader = DirectoryReader.open(ram);
      searcher = new IndexSearcher(reader);

      indexer = new IndexerTask();
      indexer.schedule(10);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }

  /**
   * create a query
   * 
   * @param s
   *          the word
   * @param fields
   *          the search fields
   * @return the Query
   */
  public static Query parse(String s, String[] fields) {
    return parse(s, fields, QueryParser.Operator.OR);
  }

  /**
   * create a query
   * 
   * @param s
   * @param fields
   * @param op
   * @return
   */
  public static Query parse(String s, String[] fields, QueryParser.Operator op) {
    try {
      MultiFieldQueryParser parser = new MultiFieldQueryParser(fields, analyzer);
      parser.setDefaultOperator(op);
      parser.setAllowLeadingWildcard(true);
      return parser.parse(s);
    } catch (Exception e) {
      log.error(s, e);
    }
    return null;
  }

  /**
   * build and boolean query
   * 
   * @param q
   *          the original query
   * @param w
   *          the group conditions
   * @return
   */
  public static Query build(Query q, W w) {
    BooleanQuery q1 = new BooleanQuery();
    q1.add(q, Occur.MUST);

    List<W.Entity> list = w.getAll();
    for (W.Entity e : list) {
      String[] ss = X.split(e.value.toString(), "|");
      if (ss.length > 1) {
        BooleanQuery q2 = new BooleanQuery();
        for (String s1 : ss) {
          q2.add(new TermQuery(new Term(e.name, s1)), Occur.SHOULD);
        }
      } else if (ss.length > 0) {
        if (ss[0].startsWith("!")) {
          q1.add(new TermQuery(new Term(e.name, ss[0].substring(1))), Occur.SHOULD);
        } else {
          q1.add(new TermQuery(new Term(e.name, ss[0])), Occur.MUST);
        }
      }
    }

    q1.add(q, Occur.MUST);
    return q1;
  }

  /**
   * searching
   * 
   * @param type
   *          the type
   * @param q
   *          the query that created by the parse API
   * @return TopDocs of the search result
   */
  public static TopDocs search(String type, Query q) {

    /**
     * avoid the searcher was changed by indexer
     */
    IndexSearcher searcher = SE.searcher;

    TimeStamp t = TimeStamp.create();
    try {
      BooleanQuery b = new BooleanQuery(); // for quest
      b.add(new TermQuery(new Term(_TYPE, type)), Occur.MUST);
      b.add(q, Occur.MUST);

      return searcher.search(b, MAX_RESULTS);

    } catch (Exception e) {
      log.error(e.getMessage(), e);
    } finally {
      search(type, t.past(), 1);
    }
    return null;
  }

  /**
   * highlight the fields with the formatter and the query, and return the
   * result
   * 
   * @param docId
   *          the docid
   * @param field
   *          the field
   * @param query
   *          the query
   * @param formatter
   *          the formatter, if null then using default formatter
   * @return the string of result
   */
  public static String highlight(int docId, String field, Query query,
      org.apache.lucene.search.highlight.Formatter formatter) {
    if (formatter == null) {
      formatter = new BoldFormatter();
    }

    Highlighter highlighter = new Highlighter(formatter, new QueryScorer(query));
    highlighter.setTextFragmenter(new SimpleFragmenter(100));

    try {
      Document doc = searcher.doc(docId);
      String text = doc.get(field);
      return highlighter.getBestFragment(analyzer, field, text);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
    return null;
  }

  /**
   * get the object id by the docid
   * 
   * @param docID
   * @return Object of id
   */
  public static <T> T get(int docID) {
    try {
      Document d = searcher.doc(docID);
      return (T) d.get(X.ID);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      // delete(docID);
    }

    return null;
  }

  /**
   * delete the document from the index database
   * 
   * @param type
   *          the document type
   * @param id
   *          the id of document
   */
  public static void delete(String type, String id) {

    BooleanQuery q1 = new BooleanQuery();
    try {
      q1.add(new TermQuery(new Term(_TYPE, type)), Occur.MUST);
      q1.add(new TermQuery(new Term(X.ID, id)), Occur.MUST);

      writer.deleteDocuments(q1);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }

  /**
   * register a searchable
   * 
   * @param type
   *          the data type
   * @param s
   *          the indexer for the searchable object
   */
  public static void register(String type, Indexer s) {
    searchables.put(type, s);

    // launch the index
    if (indexer != null) {
      indexer.schedule(0);
    }

  }

  private static Map<String, Indexer> searchables = new HashMap<String, Indexer>();

  /**
   * the index interface, used to created index for candidates searchable object
   * 
   * @author wujun
   *
   */
  public static interface Indexer {

    /**
     * get next object id
     * 
     * @param flag
     * @return Object of id
     */
    Object next(long flag);

    /**
     * load the Document by object id
     * 
     * @param id
     * @return
     */
    Document load(Object id);

    /**
     * mark the object has been done
     * 
     * @param id
     */
    void done(Object id, long flag);

    /**
     * mark the object occur error
     * 
     * @param id
     */
    void bad(Object id, long flag);

  }

  /**
   * resort all
   */
  public static void reset() {
    try {
      FLAG = System.currentTimeMillis();

      counter.clear();

      ram = new RAMDirectory();
      writer = new IndexWriter(ram, new IndexWriterConfig(Version.LATEST, analyzer));
      writer.commit();

      IndexReader reader = DirectoryReader.open(ram);
      searcher = new IndexSearcher(reader);

    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }

  private static void error(String type, int n) {
    Counter c = counter.get(type);
    if (c == null) {
      c = new Counter();
      counter.put(type, c);
    }
    c.error += n;
  }

  private static void index(String type, long cost, int n) {
    Counter c = counter.get(type);
    if (c == null) {
      c = new Counter();
      counter.put(type, c);
    }
    c.indexcost += cost;
    c.indextimes += n;
  }

  private static void search(String type, long cost, int n) {
    Counter c = counter.get(type);
    if (c == null) {
      c = new Counter();
      counter.put(type, c);
    }
    c.searchcost += cost;
    c.searchtimes += n;
    if (cost > c.searchmax) {
      c.searchmax = cost;
    }
    if (cost < c.searchmin) {
      c.searchmin = cost;
    }
  }

  final private static class IndexerTask extends Task {

    @Override
    public String getName() {
      return "se.indexer";
    }

    @Override
    public void onExecute() {
      boolean updated = false;

      for (String type : searchables.keySet().toArray(new String[searchables.size()])) {
        Indexer s = searchables.get(type);
        synchronized (s) {
          Object prev = null;
          Object id = s.next(FLAG);
          while (!X.isEmpty(id)) {
            try {
              if (X.isSame(id, prev)) {
                s.bad(id, FLAG);
                error(type, 1);
                log.warn("load same id in one time, id=" + id);
              } else {
                TimeStamp t = TimeStamp.create();
                Document d = s.load(id);
                if (d != null) {
                  d.add(new StringField(_TYPE, type, Store.NO));
                  d.add(new StringField(X.ID, id.toString(), Store.YES));

                  BooleanQuery q = new BooleanQuery();
                  q.add(new TermQuery(new Term(_TYPE, type)), Occur.MUST);
                  q.add(new TermQuery(new Term(X.ID, id.toString())), Occur.MUST);
                  writer.deleteDocuments(q);

                  writer.addDocument(d);
                  s.done(id, FLAG);
                  updated = true;
                  prev = id;

                  index(type, t.past(), 1);
                } else {
                  log.warn("bad id ?" + id);
                  s.bad(id, FLAG);
                  error(type, 1);
                }
              }
            } catch (Exception e) {
              log.error(e.getMessage(), e);
              s.bad(id, FLAG);
              error(type, 1);
            }
            id = s.next(FLAG);
          }
        }
      }

      if (updated) {
        try {
          writer.commit();
          IndexReader reader = DirectoryReader.open(ram);
          searcher = new IndexSearcher(reader);
        } catch (Exception e) {
          log.error(e.getMessage(), e);
        }
      }
    }

    @Override
    public void onFinish() {
      this.schedule(X.AMINUTE);
    }

  }

  private static class Counter {
    float indexcost;
    int   error;
    int   indextimes;
    float searchcost;
    int   searchtimes;
    long  searchmax;
    long  searchmin = Long.MAX_VALUE;

  }
}
