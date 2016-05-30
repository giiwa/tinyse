package org.giiwa.tinyse.se;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
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

	static final long FLAG = System.currentTimeMillis();

	private static RAMDirectory ram;
	private static IndexWriter writer;
	private static IndexSearcher searcher;

	public static void init(Configuration conf) {
		try {
			ram = new RAMDirectory();
			writer = new IndexWriter(ram, new IndexWriterConfig(new IKAnalyzer()));
			writer.commit();

			IndexReader reader = DirectoryReader.open(ram);
			searcher = new IndexSearcher(reader);

			new IndexerTask().schedule(10);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
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

		try {
			BooleanQuery.Builder b = new BooleanQuery.Builder(); // for quest
			b.add(new TermQuery(new Term("_type", "rule")), Occur.MUST);
			b.add(q, Occur.MUST);
			return searcher.search(b.build(), n);

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
	public static void register(String type, Searchable s) {
		searchables.put(type, s);
	}

	private static Map<String, Searchable> searchables = new HashMap<String, Searchable>();

	public static interface Searchable {

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

	final private static class IndexerTask extends Task {

		@Override
		public void onExecute() {
			boolean updated = false;

			for (String type : searchables.keySet()) {
				Searchable s = searchables.get(type);
				Object prev = null;
				Object id = s.next(FLAG);
				while (!X.isEmpty(id)) {
					try {
						if (X.isSame(id, prev)) {
							s.bad(id, FLAG);
							log.warn("load same id in one time, id=" + id);
						} else {
							Document d = s.load(id);
							if (d != null) {
								d.add(new StringField("_type", type, Store.NO));
								d.add(new StringField("_id", id.toString(), Store.YES));

								writer.deleteDocuments(new Term("_id", id.toString()));
								writer.addDocument(d);
								s.done(id, FLAG);
								updated = true;
								prev = id;
							} else {
								s.bad(id, FLAG);
							}
						}
					} catch (Exception e) {
						log.error(e.getMessage(), e);
						s.bad(id, FLAG);
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
}
