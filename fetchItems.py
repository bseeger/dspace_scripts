#!/bin/python3

import json
import requests
import csv
from os import path


# Quick script to gather some items from DSpace to ingest into
# Islandora 8.  It is a work in progress.

# author: bseeger@jhu.edu
# since: March 2020

from ingest_1_uuid import item_uuids as uuids_1
from ingest_2_uuid import item_uuids as uuids_2
#
item_uuids = uuids_1 + uuids_2

# or
#item_uuids = [
#   "3285513f-ea01-458f-a089-446e2493f255",
#   "7ba72efa-0b7c-409c-9041-2c03a31e0d1a",
#   "58f1ec2e-e681-4c5d-8c2d-6d9def4177bf",
#   "f3a750f0-45c6-40a4-9d94-36451f4b3bce",
#   "cebd28c8-b92e-478f-aa4c-c3047865f121"
#   ]

verbose = False
data_dir = "./binaries"
drupal_data_dir = "/var/www/html/drupal/web/modules/contrib/jhu_ingest_csv/data/files"
rest_url = "https://jscholarship.library.jhu.edu/rest/"
csv_schema = ["ID", "parent_id", "collection", "uuid", "date_accessioned", "date_available", "date issued",
              "identifier_other", "identifier_uri", "description", "language",
              "publisher", "relation", "rights", "subject", "title", "creator",
              "type", "isformatof", "model", "media_use", "mimetype", "sequence_id", "display", "file"]

def normalize_filename(filename):
    return filename.replace(' ', '_').replace('[', '_').replace(']', '_')

def handle_metadata(meta_dict, data, verbose):
    keys = meta_dict.keys() 
    data.extend(
                [
                 "" if "dc.date.accessioned" not in keys else meta_dict["dc.date.accessioned"], 
                 "" if "dc.date.available" not in keys else meta_dict["dc.date.available"],
                 "" if "dc.date.issued" not in keys else meta_dict["dc.date.issued"],
                 "" if "dc.identifier.other" not in keys else  meta_dict["dc.identifier.other"],
                 "" if "dc.identifier.uri" not in keys else meta_dict["dc.identifier.uri"],
                 "" if "dc.description.abstract" not in keys else meta_dict["dc.description.abstract"].encode('utf-8'),
                 "" if "dc.language.iso" not in keys else meta_dict["dc.language.iso"],
                 "" if "dc.publisher" not in keys else meta_dict["dc.publisher"],
                 "" if "dc.relation" not in keys else meta_dict["dc.relation"],
                 "" if "dc.rights" not in keys else meta_dict["dc.rights"],
                 "" if "dc.subject" not in keys else meta_dict["dc.subject"],
                 "" if "dc.title" not in keys else meta_dict["dc.title"],
                 "" if "dc.creator" not in keys else meta_dict["dc.creator"],
                 "" if "dc.type" not in keys else meta_dict["dc.type"],
                 "" if "dc.relation.isformatof" not in keys else meta_dict["dc.relation.isformatof"],
                ])


#def handle_bitstream(bin_info, data, dir, rest_url, verbose, meta_dict):
    # pull the retrieve link from json
    # download the file to 'dir' directory
    # note the name of the file in the csv file

   


if __name__ == '__main__':

    id = 0

    with open('./JHUTestIngest.csv', 'w') as csvfile:

        writeCSV = csv.writer(csvfile)
        writeCSV.writerow([r for r in csv_schema])


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

            item_url_bin_info = rest_url + '/items/' + uuid + '/bitstreams?limit=100'
            resp = requests.get(item_url_bin_info)
            if resp.status_code != requests.codes.ok:
                print("failed to get bitstream info for ({}): {}".format(resp.status_code, item_url_bin_info))
                resp.raise_for_status()
            bininfo = resp.json()
            if verbose:
                print(bininfo)

            parent_id = ""  # TODO fix me, place holder for now
            collection = "" # TODO fix me, place holder for now
            data = [id, parent_id, collection, uuid]

            # Parse the metadata into approprate information to put into the csv file. 

            meta_dict = {}
            for i in meta:
                if i['key'] in meta_dict:
                    meta_dict[i['key']] = meta_dict[i['key']] + '|' + i['value']
                    print("key '{}' is now: {}".format(i['key'], meta_dict[i['key']]))
                else: 
                    meta_dict[i['key']] = i['value']


            handle_metadata(meta_dict, data, verbose)

            # one row for every binary just to see what it looks like
            for bs in bininfo:
                url = rest_url + bs['retrieveLink'][6:] 
                
                filename = normalize_filename(bs['name'])
                file_loc = path.join(data_dir, filename)
                if verbose:
                    print("Storing binary in: '{}'".format(file_loc))

                # save everything right now, but if you want to exclude some, put the 
                # mimetypes in the array
                if bs['mimeType'] not in ["put types here to exclude"]:
                    # get file via requests
                    resp = requests.get(url)
                    if resp.status_code != requests.codes.ok:
                        print("There was an issue retrieving the file ({}): '{}'".format(resp.status_code, url))
                        resp.raise_for_status()
                 
                    # store file (may be one step)
                    with open(file_loc, 'wb') as fp:
                        fp.write(resp.content)
                else:
                    print ("Skipping {}".format(bs['mimeType']))

                drupal_file_loc = path.join(drupal_data_dir, filename)

                # TODO:
                # 1) add logic to figure out display hint based on mimetype
                # 2) add logic to figure out model 
                # 3) If item has more then one page, insert a Paged-Content object with all metadata first. 
                if verbose:
                    print(data + [bs['bundleName'], bs['mimeType'], bs['sequenceId'],file_loc])
                writeCSV.writerow(data + ["MODEL", bs['bundleName'], bs['mimeType'], bs['sequenceId'], "DISPLAY", drupal_file_loc])


    print("Done!")
