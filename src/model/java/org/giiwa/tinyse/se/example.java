package org.giiwa.tinyse.se;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.giiwa.core.bean.Bean;
import org.giiwa.core.bean.DBMapping;

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

		@Override
		public Object next(long flag) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Document load(Object id) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void done(Object id) {
			// TODO Auto-generated method stub

		}

		@Override
		public void bad(Object id) {
			// TODO Auto-generated method stub

		}
	}
}
