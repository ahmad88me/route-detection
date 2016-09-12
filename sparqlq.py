from SPARQLWrapper import SPARQLWrapper, JSON
# import pprint
# pp = pprint.PrettyPrinter(indent=4)


def run_sparql_query(query, endpoint=None, lang=None):
    """
    Assuming the query only returns a single column
    :param query:
    :param endpoint:
    :return: a list of dicts, each with a variable key as the requested one in the select. The second thing is the key
              e.g.  [dict1, dict2, ...], key
              dict1 [key] = value1
              dict2 [key] = value2
              ...
    """
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    try:
        results = sparql.query().convert()
    except Exception as e:
        print "sparql error: $$<%s>$$" % str(e)
        print "query: $$<%s>$$" % str(query)
        return []
    col = []
    keys = results["head"]["vars"]
    print results["head"]["vars"]
    for result in results["results"]["bindings"]:
        row = []
        for key in keys:
            if lang and 'xml:lang' in result[key]:
                if lang == result[key]['xml:lang']:
                    row.append(result[key]['value'])
            else:
                row.append(result[key]['value'])
        col.append(row)
    print "query"
    #print query
    print "col"
    #print col
    print "\n\n=====\n\n=========="
    return col
    # return results["results"]["bindings"], results["head"]["vars"][0]


def get_properties_of_concept(concept, endpoint=None, lang=None):
    query = """
        SELECT DISTINCT ?p where {
        ?x a %s .
        %s  ?p ?z .
        }
        limit 20
    """ % (concept, concept)
    return run_sparql_query(query=query, endpoint=endpoint, lang=lang)


def get_property_values_of_concept(concept, property, endpoint=None, lang=None):
    query = """
        SELECT ?z where {
        ?x a %s .
        ?x  %s ?z .
        }
    """ % (concept, property)
    return run_sparql_query(query=query, endpoint=endpoint, lang=lang)

