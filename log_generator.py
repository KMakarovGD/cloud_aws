#!/usr/bin/python3

# -*- coding: utf-8 -*-
"""
Created on M

generates logs about electronic goods

writes to streams via Kinesis agent

It contains the catalog of items (with item’s title, description, category and user’s reviews)

@author: Makarov Konstantin
"""

import csv
import time
from datetime import datetime
import sys
import random

from typing import List

sourceData = "items.csv"
placeholder = "LastLine.txt"

TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def get_review_texts(path):
    """
    generates rewies text
    :param path:
    :return:
    """

    with open(path) as f:
        lines = [line.split("\t", 1)[-1].lower() for line in f.read().splitlines()]

    return lines


REVIEWS = get_review_texts("review.txt")


def GetLineCount():
    with open(sourceData) as f:
        for i, l in enumerate(f):
            pass
    return i


def _process_row(row: str) -> List[str]:
    """
    make a correct row data [item_id, timestamp, device_type,
    device_id, user_ip, review_title, review_text, review_stars]
    """

    user_ip = f"{random.randint(11, 191)}.{random.randint(1, 223)}." \
              f"{random.randint(1, 254)}.{random.randint(1, 254)}"
    device_type = random.choice(["mobile:ios", "mobile:android", "other"])
    device_id = random.randint(1, 1000000) if device_type != "other" else "NULL"
    item_id = row[0]

    review = random.choice(REVIEWS).split()
    review_title = " ".join(review[:3])
    review_text = " ".join(review[3:])

    timestamp = datetime.now().strftime(TS_FORMAT)
    review_stars = random.randint(0, 5)
    result_row = [item_id, timestamp, device_type,
                  device_id, user_ip, review_title, review_text, review_stars]

    return result_row


def MakeLog(startLine, numLines):
    destData = time.strftime("/var/log/cadabra/%Y%m%d-%H%M.log") #kinesys agent works with that dir
    with open(sourceData, 'r') as csvfile:
        with open(destData, 'w') as dstfile:
            reader = csv.reader(csvfile)
            writer = csv.writer(dstfile)
            next(reader)  # skip header
            inputRow = 0
            linesWritten = 0
            for row in reader:
                inputRow += 1
                if (inputRow > startLine):
                    writer.writerow(_process_row(row))
                    linesWritten += 1
                    if (linesWritten >= numLines):
                        break
            return linesWritten


numLines = 100
startLine = 0
if (len(sys.argv) > 1):
    numLines = int(sys.argv[1])

try:
    with open(placeholder, 'r') as f:
        for line in f:
            startLine = int(line)
except IOError:
    startLine = 0

print("Writing " + str(numLines) + " lines starting at line " + str(startLine) + "\n")

totalLinesWritten = 0
linesInFile = GetLineCount()

while (totalLinesWritten < numLines):
    linesWritten = MakeLog(startLine, numLines - totalLinesWritten)
    totalLinesWritten += linesWritten
    startLine += linesWritten
    if (startLine >= linesInFile):
        startLine = 0

print("Wrote " + str(totalLinesWritten) + " lines.\n")

with open(placeholder, 'w') as f:
    f.write(str(startLine))
