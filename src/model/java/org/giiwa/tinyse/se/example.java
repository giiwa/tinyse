package org.giiwa.tinyse.se;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.giiwa.core.bean.Bean;
import org.giiwa.core.bean.DBMapping;
import org.giiwa.core.bean.UID;
import org.giiwa.core.bean.X;

import com.mongodb.BasicDBObject;

public class example {

	public static void main(String[] args) {
		
		
		SE.register("test", new Example());

		////
		Query q = new TermQuery(new Term("name", "ss"));

		TopDocs docs = SE.search("test", q, 10);

	}

	@DBMapping(collection = "example")
	public static class Example extends Bean implements SE.Searchable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public static void create(V v) {
			long id = UID.next("example.id");
			while(Bean.exists(new BasicDBObject(X._ID, id), Example.class)) {
				id = UID.next("example.id");
			}
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
