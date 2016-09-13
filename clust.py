from sklearn.cluster import KMeans

import matplotlib.pyplot as plt
from matplotlib import colors as matplot_colors
import six

colors = list(six.iteritems(matplot_colors.cnames))


def do_clustering(data):
    """
    :param data: two dimensional array, each row with a stopbus and a datetime both represented as integers
    :return:
    """
    num_of_clusters = len(data)
    model = KMeans(n_clusters=num_of_clusters)
    print len(data)
    print len(data[0])
    model.fit(data)
    #bus_station_axis, time_axis = zip(*data)
    #plt.scatter(x[:5],y[:5])
    print len(data)
    for idx, bbb in enumerate(data):
        bus_station_point, time_point = bbb
        for clus in range(num_of_clusters):
            #print members
            #plt.scatter(bus_station_axis, time_axis, colors[clus])
            if model.labels_[idx] == clus:
                cc = colors[clus]
                plt.scatter([time_point],[bus_station_point], c=cc)
    for clus in range(num_of_clusters):
        y, x = model.cluster_centers_[clus]
        plt.scatter([x], [y], c=colors[clus], marker="x", s=80)
    for bus, time_string in data:
        plt.annotate(bus, (time_string, bus))
    for idx, xy in enumerate(data[:-1]):
        plt.plot([data[idx][1], data[idx+1][1]], [data[idx][0], data[idx+1][0]])
    #plt.scatter(bus_station_axis, time_axis)
    plt.show()

