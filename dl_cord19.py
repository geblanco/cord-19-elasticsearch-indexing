#!/usr/bin/env python
# Taken from:
# https://github.com/castorini/anserini/blob/master/src/main/python/trec-covid/index_cord19.py
import os
import shutil
import tarfile
import requests
import argparse

from bs4 import BeautifulSoup
from tqdm import tqdm
from pathlib import Path
from urllib.request import urlretrieve

from indexing.preprocessing.fix_dates import fix_dates
from indexing.process_cord import main as process_cord
from indexing.process_cord import get_parser as get_indexing_parser


class TqdmUpTo(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)  # will also set self.n = b * bsize


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date", type=str, metavar="YYYY-MM-DD", required=False,
        help="Date of the CORD-19 release (required if not scrapping latest)"
    )
    parser.add_argument(
        "--scrape_latest", action="store_true",
        help="Whether to scrape the latest version (does not require --data)"
    )
    parser.add_argument(
        "--all",  action="store_true",
        help="Download, index, and verify a CORD-19 release."
    )
    parser.add_argument(
        "--download",  action="store_true",
        help="Download a CORD-19 release."
    )
    parser.add_argument(
        "--index",  action="store_true",
        help="Build abstract, full-text, and paragraph indexes."
    )
    parser.add_argument(
        "--force",  action="store_true",
        help="Overwrite existing data."
    )
    return get_indexing_parser(parser, requires=False)


def parse_flags():
    parser = get_parser()
    args = parser.parse_args()
    if args.date is None and not args.scrape_latest:
        raise ValueError(
            "You must either scrape latest release or provide a date!"
        )

    return args


def scrape_latest():
    base_url = "https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/historical_releases.html"
    r = requests.get(base_url, allow_redirects=True)
    if r.status_code != 200:
        raise RuntimeError("Unable to scrape ai2 index page!")

    soup = BeautifulSoup(r.content, "html.parser")
    infos = [i.find_all(text=True)[0] for i in soup.find_all("i")]
    latest = [i for i in infos if "latest release:" in i.lower()][0]
    date = latest.split(":")[-1].strip()
    if len(date.split("-")) != 3:
        raise RuntimeError(
            "Unable to parse date from ai2 index!"
            "\n".join(infos)
        )

    return date


def download_url(url, save_dir):
    filename = url.split("/")[-1]
    bar = TqdmUpTo(
        unit="B", unit_scale=True, unit_divisor=1024, miniters=1, desc=filename
    )
    save_dir = Path(save_dir)
    if not save_dir.exists():
        save_dir.mkdir(parents=True, exist_ok=True)

    filename_path = str(save_dir.joinpath(filename))
    with bar as t:
        urlretrieve(url, filename=filename_path, reporthook=t.update_to)


def download_collection(date):
    print(f"Downloading CORD-19 release of {date}...")
    collection_dir = "data/"
    base_url = "https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/historical_releases"  # noqa: E501
    tarball_url = f"{base_url}/cord-19_{date}.tar.gz"
    tarball_local = os.path.join(collection_dir, f"cord-19_{date}.tar.gz")

    if not os.path.exists(tarball_local):
        print(f"Fetching {tarball_url}...")
        download_url(tarball_url, collection_dir)
    else:
        print(f"{tarball_local} already exists, skipping download.")

    print(f"Extracting {tarball_local} into {collection_dir}")
    tarball = tarfile.open(tarball_local)
    tarball.extractall(collection_dir)
    tarball.close()

    docparses = os.path.join(collection_dir, date, "document_parses.tar.gz")
    collection_base = os.path.join(collection_dir, date)

    print(f"Extracting {docparses} into {collection_base}...")
    tarball = tarfile.open(docparses)
    tarball.extractall(collection_base)
    tarball.close()

    print(f"Renaming {collection_base}")
    os.rename(collection_base, os.path.join(collection_dir, f"cord19-{date}"))


def build_indexes(date, args):
    # fix dates in metadata archive
    data_dir = f"data/cord19-{date}"
    orig_meta = f"data/cord19-{date}/metadata.csv"
    corr_meta = f"data/cord19-{date}/metadata_dates.csv"
    fix_dates(orig_meta, corr_meta)
    process_cord(
        metadata=corr_meta,
        data_dir=data_dir,
        address=args.address,
        port=args.port,
        incl_abs=args.incl_abs,
        batch_size=args.batch_size
    )


def main(args):
    if not args.all and not (args.download or args.index):
        print("Must specify --all or one of {--download, --index}.")
    else:
        date = args.date
        if args.scrape_latest:
            date = scrape_latest()

        if args.all or args.download:
            collection_dir = f"data/cord19-{date}"
            if not args.force and os.path.exists(collection_dir):
                print(
                    "Collection exists; not redownloading collection. " +
                    "Use --force to remove existing collection " +
                    "and redownload."
                )
            else:
                if os.path.exists(collection_dir):
                    print("Removing existing collection...")
                    shutil.rmtree(collection_dir)
                download_collection(date)

        if args.all or args.index:
            build_indexes(date, args)


if __name__ == "__main__":
    main(parse_flags())
