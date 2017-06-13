import argparse
import csv
import hashlib
import itertools
import os
import grequests

def save_file(dir_path, data):
    hasher = hashlib.sha256()
    hasher.update(data)
    name = os.path.join(dir_path, hasher.hexdigest())
    if not os.path.exists(name):
        with open(name, 'wb') as fobj:
            fobj.write(data)

def pick_field(fieldname):
    return lambda item: item[fieldname]

def read_item(filename, limit=None):
    with open(filename, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for idx, row in enumerate(reader):
            if idx == 0:
                headers = row
            else:
                if limit is not None and idx == limit:
                    break
                yield {key: value for key, value in zip(headers, row)}

def exception_handler(request, exception):
    print 'Request failed : %s - %s' % (request.url, exception)

def main():
    parser = argparse.ArgumentParser(description='Download images from csv.')
    parser.add_argument('--field', type=str, required=True, help='Field name from which to read image url.')
    parser.add_argument('--datadir', type=str, required=True, help='Data dir to place the downloaded images.')
    parser.add_argument('--file', type=str, required=True, help='Input csv file.')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of records to process.')
    parser.add_argument('--c', type=int, default=10, help='Concurrent requests.')
    args = parser.parse_args()

    image_urls = itertools.imap(
        pick_field(args.field), 
        read_item(args.file, args.limit)
    )
    reqs = itertools.imap(grequests.get, image_urls)
    for idx, res in enumerate(grequests.imap(reqs, size=args.c, exception_handler=exception_handler)):
        if idx % 100 == 0:
            print '[D] - %s' % idx
        save_file(args.datadir, res.content)

if __name__ == '__main__':
    main()
