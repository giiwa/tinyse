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
import org.giiwa.core.bean.Helper;
import org.giiwa.core.bean.Helper.V;
import org.giiwa.core.bean.Helper.W;
import org.giiwa.core.bean.Table;
import org.giiwa.core.bean.UID;
import org.giiwa.core.bean.X;

public class example {

  public static void main(String[] args) {

    init_data();

    SE.register("test", new Example());

    ////
    Query q = new TermQuery(new Term("name", "ss"));

    TopDocs docs = SE.search("test", q);
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

  @Table(name = "example")
  public static class Example extends Bean implements SE.Indexer {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public String getName() {
      return this.getString("name");
    }

    public static void create(V v) {
      long id = UID.next("example.id");
      try {
        while (Helper.exists(id, Example.class)) {
          id = UID.next("example.id");
        }
      } catch (Exception e1) {
        log.error(e1.getMessage(), e1);
      }
    }

    public static Example load(long id) {
      return Helper.load(id, Example.class);
    }

    @Override
    public Object next(long flag) {
      Example e = Helper.load(W.create().and("flag", flag, W.OP_NEQ), Example.class);
      if (e != null) {
        return e.get(X.ID);
      }
      return null;
    }

    @Override
    public Document load(Object id) {
      Example e = Helper.load(id, Example.class);
      if (e != null) {
        Document d = new Document();
        d.add(new StringField("name", e.getString("name"), Store.NO));
        return d;
      }
      return null;
    }

    @Override
    public void done(Object id, long flag) {
      Helper.update(id, V.create("flag", flag).set("indexerror", ""), Example.class);
    }

    @Override
    public void bad(Object id, long flag) {
      Helper.update(id, V.create("flag", flag).set("indexerror", "error"), Example.class);
    }
  }
}
