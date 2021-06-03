#!/usr/bin/python

"""
Gets from City, State, USA pulled from csv argv[1], outputs to csv argv[2] with coords appended

"""

import sys
import csv
import requests
import multiprocessing

def find_coords(row):
    url = "https://nominatim.openstreetmap.org/search?q=" + row[3].replace(' ', '+') + ',+USA+' + "&format=jsonv2"
    response = requests.get(url)
    response = response.json()
    out = []
    try:
        out = row + [response[0]['lat']] + [response[0]['lon']]
    except:
        pass
    print(out)
    return out

if __name__ == "__main__":
    rows = []
    with open(sys.argv[1], 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"')
        rows = [row for row in csv_reader]
    
    pool = multiprocessing.Pool(8)
    out = pool.map(find_coords, (row for row in rows))

    with open(sys.argv[2], 'w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"')
        csv_writer.writerows(out)
        