package org.giiwa.tinyse.se;

import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.TokenGroup;

public class BoldFormatter implements Formatter {

  public String highlightTerm(String originalText, TokenGroup group) {
    if (group.getTotalScore() <= 0) {
      return originalText;
    }

    return new StringBuilder("<r>").append(originalText).append("</r>").toString();
  }
}
