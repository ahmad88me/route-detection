
from sparqlq import run_sparql_query
from clust import do_clustering
from operator import itemgetter
import pandas as pd

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
    return res


def print_sparql_result(res):
    for r in res:
        print r


def buses_as_seq_ints(data):
    stations = zip(*data)[0]
    set_of_stations = set(stations)
    unique_stations = list(set_of_stations)
    mappings = {}
    print "unique stations: "+str(unique_stations)
    for idx, station in enumerate(unique_stations):
        mappings[station] = idx * 1000000 + 99999999

    # for m in mappings:
    #     print "%d => %d" % (m, mappings[m])

    clean_data = []
    for d in data:
        old_station = d[0]
        new_station = mappings[old_station]
        clean_data.append([new_station, d[1]])
    return clean_data


def get_only_first_validation(data):
    station_dict = {}
    for d in data:
        if d[0] in station_dict:
            station_dict[d[0]] = min(d[1], station_dict[d[0]])
        else:
            station_dict[d[0]] = d[1]
    clean_data = station_dict.items()
    clean_data = sorted(clean_data, key=itemgetter(1))
    print "\n\n\nclean data:*************"
    print clean_data
    return clean_data


def get_only_first_validation_dataframe(data):
    station_dict = {}
    for d in data.iterrows():
        if d[1][0] in station_dict:
            station_dict[d[1][0]] = min(d[1][1], station_dict[d[1][0]])
        else:
            station_dict[d[1][0]] = d[1][1]
    clean_data = station_dict.items()
    clean_data = sorted(clean_data, key=itemgetter(1))
    print "\n\n\nclean data:*************"
    print clean_data
    return clean_data


def clean_the_data(data):
    """
    :param data: two dimensional each row contain a bus station and a timestamp
    :return: bus station as a number and a timestamp as a number
    """
    clean_data = []
    for r in data:
        new_station = r[0].replace('http://crtm.linkeddata.es/recurso/transporte/validacion/dpaypoint/99_L447_P', '')
        new_station = int(new_station) #* 1000 #* 1000000000
        # if new_station != 420:
        #     continue
        # 2016-03-10T22:56:26+02:00
        new_time = "".join(r[1].split('T')[0].split('-'))
        if new_time != '20160310':
            continue
        # new_time += "".join(r[1].split('T')[1].split('+')[0].split(':'))
        new_time = "".join(r[1].split('T')[1].split('+')[0].split(':'))
        new_time = int(new_time)
        # if new_time < 20160310000000: # to filter the data for a single day
        #     continue
        clean_data.append([new_station, new_time])
    # clean_data = buses_as_seq_ints(clean_data)
    # clean_data = get_only_first_validation(clean_data)
    return clean_data


def save_data_as_csv(data, fname):
    f = open(fname, "w")
    for d in data:
        f.write(d[0]+", "+d[1]+"\n")
    f.close()


def tab_separated_to_csv():
    data = []
    fname = "crtm-linea447.txt"
    with open(fname, "r") as f:
        for idx, line in enumerate(f.readlines()):
            if idx%2==0:
                # print "skip: "+str(line)
                continue
            else:
                print "line: "+str(line)
            #print len(line.split('\t'))
            _, station, timestring = line.split('\t')
            #print "station: "+station+", time: "+timestring
            data.append([station, timestring])
    clean_data = clean_the_data(data)
    with open("data_line_447.csv", "w") as f:
        for d in clean_data:
            f.write(str(d[0])+", "+str(d[1])+"\n")


def print_data_frames(data):
    for d in data.iterrows():
        print d[1].values


def get_duration_between_stations(data):
    dur = {}
    for idx, d in enumerate(data[:-1]):
        if d[0] not in dur:
            dur[d[0]] = {}
        if data[idx+1][0] not in dur:
            dur[data[idx+1][0]] = {}
        print "d1: %d" % d[1]
        print "data idx: %d" % data[idx+1][1]
        dur[d[0]][data[idx+1][0]] = abs(d[1] - data[idx+1][1])
        dur[data[idx+1][0]][d[0]] = abs(d[1] - data[idx+1][1])
    with open("durations.csv", "w") as f:
        f.write(" ___ ")
        for header in dur.keys():
            f.write(", "+str(header))
        for d in dur:
            for idx, r in enumerate(dur):
                if idx==0:
                    f.write(str(r))
                if r in dur[d]:
                    f.write(", "+str(dur[d][r]))
                else:
                    f.write(", 0")
            f.write("\n")


def main():
    # This is to fetch the data from the sparql endpoint
    #data = fetch_data_from_endpoint()
    # This is to save the data to a csv file
    #save_data_as_csv(data, "data_line_447_clean.csv")
    data = pd.read_csv("data_line_447.csv")
    #tab_separated_to_csv()
    #print_sparql_result(data)
    print_data_frames(data)
    data = get_only_first_validation_dataframe(data)
    # data = buses_as_seq_ints(data)
    # clean_data = clean_the_data(data)
    # print_sparql_result(clean_data)
    # do_clustering(data)
    get_duration_between_stations(data)


main()