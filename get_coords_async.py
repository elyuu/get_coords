#!/usr/bin/python

"""
Gets from City, State, USA pulled from csv argv[1], outputs to csv argv[2] with coords appended
Async version is too fast for OSM nominatim endpoint
"""

import sys
import csv
import aiohttp
import asyncio

async def get_coords(url, row, session):
    out = []
    async with session.get(url) as req:
        req.raise_for_status()
        response = await req.json()
        try:
            out = row + [response[0]['lat']] + [response[0]['lon']]
            print(out)
        except:
            pass
    return out

async def main(rows):
    async with aiohttp.ClientSession() as session:
        reqs = []
        for row in rows:
            url = "https://nominatim.openstreetmap.org/search?q=" + row[3].replace(' ', '+') + ',+USA+' + "&format=jsonv2"
            reqs.append(get_coords(url, row, session))
        res = await asyncio.gather(*reqs)
        return res

if __name__ == "__main__":
    rows = []
    with open(sys.argv[1], 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"')
        rows = [row for row in csv_reader]
    
    loop = asyncio.get_event_loop()
    out = loop.run_until_complete(main(rows))

    with open(sys.argv[2], 'w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"')
        csv_writer.writerows(out)