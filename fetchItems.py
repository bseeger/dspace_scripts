#!/bin/python3

import json
import requests
import csv
from os import path


# Quick script to gather some items from DSpace to ingest into
# Islandora 8.  It is a work in progress.

# author: bseeger@jhu.edu
# since: March 2020

item_uuids = [
    "3285513f-ea01-458f-a089-446e2493f255",
    "7ba72efa-0b7c-409c-9041-2c03a31e0d1a",
    # more UUIDs here
]

verbose = False
data_dir = "./images"
rest_url = "https://jscholarship.library.jhu.edu/rest/"

def handle_metadata(meta_dict, data, verbose):
    keys = meta_dict.keys() 
    # todo - make this more robust to handle the case where one of the items doesn't exist
    data.extend(
                [
                 "" if "dc.date.accessioned" not in keys else meta_dict["dc.date.accessioned"], 
                 "" if "dc.date.available" not in keys else meta_dict["dc.date.available"],
                 "" if "dc.date.issued" not in keys else meta_dict["dc.date.issued"],
                 "" if "dc.identifier.other" not in keys else  meta_dict["dc.identifier.other"],
                 "" if "dc.identifier.uri" not in keys else meta_dict["dc.identifier.uri"],
                 "" if "dc.description.abstract" not in keys else meta_dict["dc.description.abstract"],
                 "" if "dc.language.iso" not in keys else meta_dict["dc.language.iso"],
                 "" if "dc.publisher" not in keys else meta_dict["dc.publisher"],
                 "" if "dc.relation" not in keys else meta_dict["dc.relation"],
                 "" if "dc.rights" not in keys else meta_dict["dc.rights"],
                 "" if "dc.subject" not in keys else meta_dict["dc.subject"],
                 "" if "dc.title" not in keys else meta_dict["dc.title"],
                 "" if "dc.type" not in keys else meta_dict["dc.type"]
                ])


def handle_bitstream(bin_info, data, dir, rest_url, verbose, meta_dict):
    # pull the retrieve link from json
    # download the file to 'dir' directory
    # note the name of the file in the csv file
    i = 0 
    for bs in bin_info:
        print("FILES: looking at '{}'".format(bs['bundleName']))
        # quick and dirty check
        if i > 1:
            print("there is more the one original file on uuid: {} ({})"
                    .format(bs['uuid'], bs['description']))

        if bs['bundleName'] == "ORIGINAL":
            # hack. yuck. Only grab the audio for this mime type (for now, as
            # there are transcriptions there as well to be considered later)
            # I will fix this once I better know what my goals for this are. 
            if (meta_dict['dc.format.mimetype'] == 'audio/mpeg' and 
               (bs['description'] != 'audio' or bs['description'] != "audio 1")):
                continue;

            i += 1
            url = rest_url + bs['retrieveLink'][6:] 
            if verbose:
                print("looking for original bit stream at: '{}'".format(url))

            file_loc = path.join(dir, bs['name'])
            if verbose:
                print("Storing data in: '{}'".format(file_loc))

            # get file via requests
            resp = requests.get(url)
            if resp.status_code != requests.codes.ok:
                print("There was an issue retrieving the file ({}): '{}'".format(resp.status_code, url))
                resp.raise_for_status()

            # store file (may be one step)
            with open(file_loc, 'wb') as fp:
                fp.write(resp.content)

            # this will not go well if there is more the one ORIGINAL file.  :-)  Time will tell...
            data.extend([file_loc])



if __name__ == '__main__':

#    with open("config.yml", 'r') as ymlfile:
#        cfg = yaml.load(ymlfile)

    schema = ["ID", "uuid", "date_accessioned", "date_available", "date issued",
              "identifier_other", "identifier_uri", "abstract", "language",
              "publisher", "relation", "rights", "subject", "title",
              "type"]

    id = 0


    with open('./JHUTestIngest.csv', 'w') as csvfile:

        writeCSV = csv.writer(csvfile)
        writeCSV.writerow([r for r in schema])


        for uuid in item_uuids:
            id += 1
            print("Working with item ({}): {}".format(id, uuid))

            # https://jscholarship.library.jhu.edu/rest/items/3285513f-ea01-458f-a089-446e2493f255/metadata    --> get json file
            # https://jscholarship.library.jhu.edu/rest/items/3285513f-ea01-458f-a089-446e2493f255/bitstreams  --> get json file

            item_url_meta = rest_url + '/items/' + uuid + '/metadata'
            resp = requests.get(item_url_meta)
            if resp.status_code != requests.codes.ok:
                print("failed to get metadata for ({}): {}".format(resp.status_code, item_url_meta))
                resp.raise_for_status()
            meta = resp.json()
            if verbose:
                print(meta)

            item_url_bin_info = rest_url + '/items/' + uuid + '/bitstreams'
            resp = requests.get(item_url_bin_info)
            if resp.status_code != requests.codes.ok:
                print("failed to get bitstream info for ({}): {}".format(resp.status_code, item_url_bin_info))
                resp.raise_for_status()
            bininfo = resp.json()
            if verbose:
                print(bininfo)

            data = [id, uuid]

            # Parse the metadata into approprate information to put into the csv file. 

            meta_dict = {}
            for i in meta:
                meta_dict[i['key']] = i['value']

            handle_metadata(meta_dict, data, verbose)
            handle_bitstream(bininfo, data, data_dir, rest_url, verbose, meta_dict)

            if verbose:
                print(data)
            writeCSV.writerow(data)

    print("Done!")
