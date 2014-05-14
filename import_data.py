from neo4jrestclient.client import GraphDatabase


synonyms = 'data/EVEX_synonyms_9606.tab'
relations = 'data/EVEX_relations_9606.tab'
articles = 'data/EVEX_articles_9606.tab'

gdb = GraphDatabase("http://localhost:7474/db/data/")

with open(synonyms, 'r') as f:
    next(f)
    for l in f:
        l = l[0:len(l)-1]
        [entrezgene_id, symbol_type, synonyms] = l.split('\t')
        print entrezgene_id, symbol_type, synonyms
        if symbol_type == 'synonym':
            q = '''MERGE (g :entity :genes {entrezgene_id : %s})
            ON CREATE SET g.names = ["%s"]
            ON MATCH SET g.names = g.names + ["%s"]''' % (entrezgene_id, synonyms, synonyms)
        elif symbol_type == 'official_symbol':
            q = '''MERGE (g :entity:genes {entrezgene_id : %s})
            ON CREATE SET g.name = "%s", g.names = ["%s"]
            ON MATCH SET g.name = "%s", g.names = g.names + ["%s"]''' % (entrezgene_id, synonyms, synonyms, synonyms, synonyms)
        result = gdb.query(q)
    gdb.query('''CREATE INDEX ON :genes(entrezgene_id)''')
    f.close()

with open(relations, 'r') as f:
    next(f)
    for l in f:
        l = l[0:len(l)-1]
        [event_id, source, target, confidence, negation, speculation, coarse_type, coarse_polarity, refined_type, refined_polarity] = l.split('\t')
        print event_id, source, target
        gdb.query('''MERGE (source :genes {entrezgene_id : %s})''' % (source, ))
        gdb.query('''MERGE (target :genes {entrezgene_id : %s})''' % (target, ))
        gdb.query('''MATCH (source :genes {entrezgene_id : %s}),
        (target :genes {entrezgene_id : %s})
        MERGE source-[:binary {general_event_id : %s}]->target''' % (source, target, event_id))
        gdb.query('''MERGE (r :relations {general_event_id : %s})
        ON CREATE SET r.confidence = %s, r.negation = %s, r.speculation = %s, r.coarse_type = "%s", r.coarse_polarity = "%s", r.refined_type = "%s", r.refined_polarity = "%s"''' % (event_id, confidence, negation, speculation, coarse_type, coarse_polarity, refined_type, refined_polarity))
    gdb.query('''CREATE INDEX ON :binary(general_event_id)''')
    gdb.query('''CREATE INDEX ON :relations(general_event_id)''')
    f.close()

with open(articles, 'r') as f:
    next(f)
    for l in f:
        l = l[0:len(l)-1]
        [event_id, article_id] = l.split('\t')
        print event_id, article_id
        gdb.query('''MERGE (a :articles {article_id : "%s"})''' % (article_id, ))
        gdb.query('''MATCH (r :relations {general_event_id : %s}),
        (a :articles {article_id : "%s"})
        MERGE r-[:found_in]->a''' % (event_id, article_id))
    gdb.query('''CREATE INDEX ON :articles(article_id)''')
