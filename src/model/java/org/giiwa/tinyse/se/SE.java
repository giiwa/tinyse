package org.giiwa.tinyse.se;

import java.util.HashMap;
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
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
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

	public static final int MAX_RESULTS = 10000;

	static Log log = LogFactory.getLog(SE.class);
	static final String _TYPE = "_type";
	static long FLAG = System.currentTimeMillis();

	private static RAMDirectory ram;
	private static IndexWriter writer;
	private static IndexSearcher searcher;

	private static Map<String, Counter> counter = new HashMap<String, Counter>();

	/**
	 * 
	 * @return
	 */
	public static Set<String> getTypes() {
		return searchables.keySet();
	}

	/**
	 * 
	 * @param type
	 * @return
	 */
	public static int error(String type) {
		Counter c = counter.get(type);
		return c == null ? 0 : c.error;
	}

	/**
	 * 
	 * @param type
	 * @return
	 */
	public static float index(String type) {
		Counter c = counter.get(type);
		return c == null ? 0 : c.indexcost / c.indextimes;
	}

	/**
	 * 
	 * @param type
	 * @return
	 */
	public static float search(String type) {
		Counter c = counter.get(type);
		return c == null ? 0 : c.searchcost / c.searchtimes;
	}

	/**
	 * 
	 * @param type
	 * @return
	 */
	public static long searchmax(String type) {
		Counter c = counter.get(type);
		return c == null ? 0 : c.searchmax;
	}

	/**
	 * 
	 * @param type
	 * @return
	 */
	public static long searchmin(String type) {
		Counter c = counter.get(type);
		return c == null ? 0 : c.searchmin;
	}

	/**
	 * 
	 * @param type
	 * @return
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

	private static Analyzer analyzer;

	public static void init(Configuration conf) {
		try {

			ram = new RAMDirectory();
			analyzer = new IKAnalyzer();
			writer = new IndexWriter(ram, new IndexWriterConfig(Version.LATEST, analyzer));
			writer.commit();

			IndexReader reader = DirectoryReader.open(ram);
			searcher = new IndexSearcher(reader);

			new IndexerTask().schedule(10);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}

	/**
	 * create a query
	 * 
	 * @param s
	 * @param fields
	 * @return
	 */
	public static Query parse(String s, String[] fields) {
		try {
			MultiFieldQueryParser parser = new MultiFieldQueryParser(fields, analyzer);
			parser.setDefaultOperator(QueryParser.Operator.OR);
			parser.setAllowLeadingWildcard(true);
			return parser.parse(s);
		} catch (Exception e) {
			log.error(s, e);
		}
		return null;
	}

	/**
	 * @param type
	 * @param q
	 * @param n
	 * @return TopDocs
	 */
	public static TopDocs search(String type, Query q, int n) {

		/**
		 * avoid the searcher was changed by indexer
		 */
		IndexSearcher searcher = SE.searcher;

		TimeStamp t = TimeStamp.create();
		try {
			BooleanQuery b = new BooleanQuery(); // for quest
			b.add(new TermQuery(new Term(_TYPE, type)), Occur.MUST);
			b.add(q, Occur.MUST);

			return searcher.search(b, n);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			search(type, t.past(), 1);
		}
		return null;
	}

	/**
	 * get the object id by the docid
	 * 
	 * @param docID
	 * @return Object of id
	 */
	public static Object get(int docID) {
		try {
			Document d = searcher.doc(docID);
			return d.get("_id");
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return null;
	}

	/**
	 * register a searchable
	 * 
	 * @param type
	 * @param s
	 */
	public static void register(String type, Indexer s) {
		searchables.put(type, s);
	}

	private static Map<String, Indexer> searchables = new HashMap<String, Indexer>();

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
		public void onExecute() {
			boolean updated = false;

			for (String type : searchables.keySet()) {
				Indexer s = searchables.get(type);
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
								d.add(new StringField(X._ID, id.toString(), Store.YES));

								BooleanQuery q = new BooleanQuery();
								q.add(new TermQuery(new Term(_TYPE, type)), Occur.MUST);
								q.add(new TermQuery(new Term(X._ID, id.toString())), Occur.MUST);
								writer.deleteDocuments(q);

								writer.addDocument(d);
								s.done(id, FLAG);
								updated = true;
								prev = id;

								index(type, t.past(), 1);
							} else {
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
		int error;
		int indextimes;
		float searchcost;
		int searchtimes;
		long searchmax;
		long searchmin = Long.MAX_VALUE;

	}
}
