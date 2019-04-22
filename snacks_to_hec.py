#!/usr/bin/env python
# hobbes3

# THIS SCRIPT IS SUPER HACKY AND IS MEANT FOR ONE-TIME USE.

import csv
import requests
import json
import time
import re
from datetime import datetime
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from settings import *

hec_headers = {
    "Authorization": "Splunk " + HEC_TOKEN
}

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

base_data = {
    "index": INDEX,
    "sourcetype": SOURCETYPE,
    "time": 0,
    "event": {},
}

with open("snacks.csv") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=",")

    i = 0
    for row in csv_reader:
        # First 3 rows are headers
        if i == 0:
            column_type = row
        elif i == 1:
            years = row
        elif i == 2:
            months = row
        elif i >= 3:
            base_snack = {
                "type": row[0],
                "id": row[1],
                "description": row[2],
                "quantity_type": row[3],
                "time": 0,
                "date": "",
                "quantity": 0,
                "cost": 0,
            }

            #import pdb; pdb.set_trace()

            for j, value in enumerate(row):
                # Ignore the first 4 columns
                # Only care if the column is "QtyActual" (I'll get the cost by hardcoding)
                # and if the value isn't for a total amount (for column BA)
                # and if the value isn't empty
                if j >= 4 and column_type[j] == "QtyActual" and months[j] != "Total" and value:
                    snack = base_snack.copy()

                    quantity = int(re.sub(r",", "", value))

                    year = years[j]
                    month = months[j]

                    date_str = year + " " + month
                    time = datetime.strptime(date_str, "%Y %B").timestamp()

                    # Super hardcoded since this is a hack script anyway
                    cost = float(re.sub(r"[$,]", "", row[j + 24]))

                    snack["time"] = time
                    snack["date"] = date_str
                    snack["quantity"] = quantity
                    snack["cost"] = cost

                    hec_data = base_data.copy()

                    hec_data["time"] = time
                    hec_data["event"] = snack

                    print(json.dumps(hec_data, indent=2, sort_keys=True))

                    r = requests.post(HEC_URL, headers=hec_headers, data=json.dumps(hec_data), verify=False, timeout=5)

                    print(r.text)

        i += 1
