package com.authrus.database.engine.predicate;

import com.authrus.database.engine.predicate.LikeEvaluator;

import junit.framework.TestCase;

public class LikeEvaluatorTest extends TestCase {
   
   public void testLike() {
      assertTrue(new LikeEvaluator("_erlin").like("Berlin"));
      assertTrue(new LikeEvaluator("%ia%").like("Niall Gallagher"));
      assertFalse(new LikeEvaluator("%ia%").like("John Doe"));
      assertTrue(new LikeEvaluator("_ia%").like("Niall"));
      assertFalse(new LikeEvaluator("_ia%").like("NNiall")); 
   }
   public void testLikeIgnoreCase() {
      assertTrue(new LikeEvaluator("b_rlin").like("Berlin"));
      assertTrue(new LikeEvaluator("N%ia%").like("Niall Gallagher"));
      assertFalse(new LikeEvaluator("%ia%").like("John Doe"));
      assertTrue(new LikeEvaluator("_ia%LL").like("Niall"));
      assertFalse(new LikeEvaluator("_ia%").like("NNiall")); 
   }
}
