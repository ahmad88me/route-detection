
from sparqlq import run_sparql_query


def fetch_data_from_endpoint():
    query = """
    PREFIX dpay: <http://datos.crtm.es/recurso/transporte/validacion/dpaypoint/>
    PREFIX gsp: <http://www.opengis.net/ont/geosparql#>
    PREFIX geosparql: <http://www.opengis.net/ont/geosparql#>
    PREFIX ttp: <http://vocab.linkeddata.es/datosabiertos/def/transporte/tarjeta-transporte-publico#>
    PREFIX validacionInstancia: <http://crtm.linkeddata.es/recurso/transporte/validacion/>
    PREFIX ssn: <http://purl.oclc.org/NET/ssnx/ssn>
    PREFIX ttpResultado: <http://crtm.linkeddata.es/recurso/transporte/validacion/resultado/>


    SELECT ?dPayPoint ?obsTime
    FROM NAMED <http://crtm.linkeddata.es/graph/data/interubanos-validaciones>
    FROM NAMED <http://crtm.linkeddata.es/graph/data/dpaypoints>
    WHERE {
      GRAPH <http://crtm.linkeddata.es/graph/data/interubanos-validaciones> {
        ?validacion a ttp:Validacion .
        ?validacion <http://purl.oclc.org/NET/ssnx/ssnobservedBy> ?dPayPoint .
        ?validacion ssn:observationResultTime ?obsTime .
        # FILTER regex(?dPayPoint, "L447", "i")
      }
      GRAPH <http://crtm.linkeddata.es/graph/data/dpaypoints> {
            ?dPayPoint <http://vocab.linkeddata.es/datosabiertos/def/transporte/transporte-publico#tieneLinea> <http://crtm.linkeddata.es/recurso/transporte/operador/447> .
            ?dPayPoint geosparql:hasGeometry ?geometry .
            ?geometry geosparql:asWKT ?wkt .
      }

    }
    order by  ?obsTime

    """
    res = run_sparql_query(query, endpoint="http://crtm.linkeddata.es/sparql")


def print_sparql_result(res):
    for r in res:
        print r


def main():
    fetch_data_from_endpoint()