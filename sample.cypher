MATCH (w:Work) 
RETURN count(w.synopsis) as synopsis_count, 
       count(w.synopsis_embedding) as synopsis_embedding_count,
       count(w.embedding) as embedding_count;