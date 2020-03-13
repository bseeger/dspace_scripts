#!/bin/python3

import yaml
import json
import requests
import csv

# This script simply reads data from dspace and outputs a csv file 
# containing information about the communities (and subcommunities) 
# and collections w/i those communities. Since it doesn't 
# authenticated, only public data is gathered.
#
# There is an option to include items in the collections
# by setting "items" to True in the config.yml file
#
# NOTE: this will only gather the first 100 items in a collection. 
# That can be changed, but for the purposes of this script, that's not
# addressed (yet).
#
# author: bseeger@jhu.edu
# since: March 2020
#
# How to use:
#
# First create a config.yml file like this: 
#
# general:
#     resturl: https://jscholarship.library.jhu.edu/rest/
#     items: False
#
# Then run the script
#   python3  ./getDSpaceData.py

verbose = False
community_id = 0

def output_communities(comm_json, csv, rest_url, parent, verbose, items):

    global community_id

    for community in comm_json:
        community_id += 1
        name = parent + '/' + community['name'] if parent else community['name']

        name = name + ' (' + community['uuid'] + ')'

        if verbose:
            print ("*" * 50)
            print ("Information for community: {}".format(name))

        # get all the collections
        comm_url = rest_url + '/communities/' + community['uuid'] + '/collections'
        if verbose:
            print ("quering: '{}'".format(comm_url))
        resp = requests.get(comm_url)
        if resp.status_code != 200:
            print ("Status {}: unable to get info for community '{}'"
                   .format(resp.status_code, name))
            continue

        output_collections(resp.json(), csv, rest_url, name, community_id, verbose, items)

        # see if there are any sub communities
        comm_url = rest_url + '/communities/' + community['uuid'] + '/communities'
        resp = requests.get(comm_url)
        if resp.status_code != 200:
            print ("Status {}: unable to get info for community '{}'"
                   .format(resp.status_code, name))
            continue
  
            sub_comm_json = resp.json()
            if len(sub_comm_json):
                print ("subcollections for {}({}) exist".format(community['name'], community['uuid']))
                output_communities(sub_comm_json, csv, rest_url, name, verbose, items)




def output_collections(coll_json, csv, rest_url, comm_name, comm_id, verbose, inc_items):
    collection_id = 0 

    if verbose:
        print("Collections:")

    for collection in coll_json:
        collection_id += 1
        if verbose:
            print("\t{}".format(collection['name']))
        item_list_url = rest_url + '/collections/' + collection['uuid'] + '/items'

        resp = requests.get(item_list_url)
        if resp.status_code != 200:
            print ("Status {}: unable to get item list for '{}'"
                   .format(resp.status_code, collection['name']))
            continue
        if verbose:
            print("    Items ({}):".format(collection['numberItems']))
        
        if not inc_items:
            csv.writerow(
                [
                    comm_id, 
                    comm_name,
                    collection_id,
                    collection['name'] + '(' + collection['uuid'] + ')',
                    collection['numberItems'],
                ])

        else:
            i = 0
            for item in resp.json():
                i += 1
                if verbose:
                    print("\t\t{}".format(item['name']))
                csv.writerow(
                        [
                            comm_id, 
                            comm_name,
                            collection_id,
                            collection['name'] + '(' + collection['uuid'] + ')',
                            collection['numberItems'],
                            i,
                            item['name'],
                            jscholarship + item['handle'],
                            item['uuid']
                        ])



if __name__ == '__main__':

    with open("config.yml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    for section in cfg:
        print(section)

    endpoint = cfg['general']['resturl']
    inc_items = cfg['general']['items']

    if inc_items:
        print("including the first 100 items from collection")
    else:
        print("not including items")

    schema = ["ID", "Community", "Collection ID",  "Collection",
              "Total Number of Item"]
    if inc_items:
        schema.extend(["Item Number", "Item Name", "Item URI"])

    jscholarship = "https://jscholarship.library.jhu.edu/"

    filename = './CollectionMap.csv'
    if inc_items:
        filename = './CollectionMapWithItems.csv'

    with open('./CollectionMap.csv', 'w') as csvfile:

        writeCSV = csv.writer(csvfile)
        writeCSV.writerow([r for r in schema])

        all_comm_url = endpoint + '/communities'
        print ("comm url: {}".format(all_comm_url))
        resp = requests.get(all_comm_url)

        print(resp)
        communities = resp.json()

        output_communities(communities, writeCSV, endpoint, '', verbose, inc_items)

    print("Done!")
