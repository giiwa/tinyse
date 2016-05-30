package org.giiwa.tinyse.se;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.giiwa.core.bean.Bean;
import org.giiwa.core.bean.DBMapping;
import org.giiwa.core.bean.UID;
import org.giiwa.core.bean.X;
import org.giiwa.core.bean.Bean.V;

import com.mongodb.BasicDBObject;

public class example {

	public static void main(String[] args) {

		init_data();

		SE.register("test", new Example());

		////
		Query q = new TermQuery(new Term("name", "ss"));

		TopDocs docs = SE.search("test", q, 10);
		ScoreDoc[] dd = docs.scoreDocs;
		for (ScoreDoc d : dd) {
			Long id = (Long) SE.get(d.doc);
			if (id != null) {
				Example e = Example.load((long) id);
				System.out.println(e.getName());
			}
		}
	}

	private static void init_data() {
		for (int i = 0; i < 1000; i++) {
			Example.create(V.create("name", "ss"));
		}
	}

	@DBMapping(collection = "example")
	public static class Example extends Bean implements SE.Searchable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public String getName() {
			return this.getString("name");
		}

		public static void create(V v) {
			long id = UID.next("example.id");
			while (Bean.exists(new BasicDBObject(X._ID, id), Example.class)) {
				id = UID.next("example.id");
			}
		}

		public static Example load(long id) {
			return Bean.load(id, Example.class);
		}

		@Override
		public Object next(long flag) {
			Example e = Bean.load(new BasicDBObject("flag", new BasicDBObject("$ne", flag)), Example.class);
			if (e != null) {
				return e.get(X._ID);
			}
			return null;
		}

		@Override
		public Document load(Object id) {
			Example e = Bean.load(id, Example.class);
			if (e != null) {
				Document d = new Document();
				d.add(new StringField("name", e.getString("name"), Store.NO));
				return d;
			}
			return null;
		}

		@Override
		public void done(Object id, long flag) {
			Bean.updateCollection(id, V.create("flag", flag).set("indexerror", ""), Example.class);
		}

		@Override
		public void bad(Object id, long flag) {
			Bean.updateCollection(id, V.create("flag", flag).set("indexerror", "error"), Example.class);
		}
	}
}
