import os
import sys
import csv
import json
import argparse

from tqdm import tqdm
from pathlib import Path
from collections import defaultdict
from elasticsearch.helpers import bulk

sys.path.append(os.path.dirname(__file__))
from es.indexing import (   # noqa: E402
    init_index,
    Paper,
    Paper_with_abs,
    Paragraph,
    Paragraph_with_abs,
    Abstract,
    Version,
)
from es.es_connector import get_connection  # noqa: E402


def get_parser(parser=None, requires=True):
    if parser is None:
        parser = argparse.ArgumentParser()

    parser.add_argument(
        "-m", "--metadata", type=str, default=None,
        help="Metadata file to use (instead of `<data_dir>/metatadata.csv`"
    )
    parser.add_argument(
        "-d", "--data_dir", type=str, required=requires,
        help="Directory containing cord19 data"
    )
    parser.add_argument(
        "-a", "--address", type=str, default="0.0.0.0",
        help="Elastic search address"
    )
    parser.add_argument(
        "-p", "--port", type=int, default=9200,
        help="Elastic search port"
    )
    parser.add_argument(
        "-i", "--incl_abs", action="store_true",
        help="Whether to add abstract field to <papers> and <paragraphs> "
        "wont create <abstracts> index"
    )
    parser.add_argument(
        "-bs", "--batch_size", type=int, default=100,
        help="Batch size for bulk saving"
    )

    return parser


def parse_flags():
    return get_parser().parse_args()


def flatten(array):
    ret = []
    for el in array:
        if not isinstance(el, list):
            ret.append(el)
        else:
            ret.extend(flatten(el))
    return ret


def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def bulk_save(es_conn, batch):
    data = [el.to_dict(True) for el in flatten(batch.values())]
    bulk(es_conn, data)


def base_doc_from_row(row, incl_abs=False):
    # lacks: body -> might be abstract, full text or part text
    # paragraph_id -> not all base docs include it
    params = dict(
        cord_uid=row["cord_uid"],
        title=row["title"].strip(),
        publish_time=row["publish_time"].strip(),
        url=row["url"].strip(),
        journal=row["journal"].strip(),
        authors=row["authors"].strip(),
    )
    if incl_abs:
        params.update(abstract=row["abstract"].strip())

    return params


def paragraph_from_row(row, part_idx, part_text, incl_abs):
    par_cls = Paragraph_with_abs if incl_abs else Paragraph
    par_params = base_doc_from_row(row, incl_abs)
    par_params.update(
        paragraph_id=part_idx,
        body=part_text,
    )

    return par_cls(**par_params)


def paper_from_row(row, full_text, incl_abs):
    paper_cls = Paper_with_abs if incl_abs else Paper
    paper_params = base_doc_from_row(row, incl_abs)
    paper_params.update(body=full_text)
    return paper_cls(**paper_params)


def abstract_from_row(row):
    params = base_doc_from_row(row)
    params.update(body=row["abstract"].strip())
    return Abstract(**params)


def filter_by_kwords(row=None, text=""):
    keywords = [
        "coronavirus 2019",
        "coronavirus disease 19",
        "cov2",
        "cov-2",
        "covid",
        "ncov 2019",
        "2019ncov",
        "2019-ncov",
        "2019 ncov",
        "novel coronavirus",
        "sarscov2",
        "sars-cov-2",
        "sars cov 2",
        "severe acute respiratory syndrome coronavirus 2",
        "wuhan coronavirus",
        "wuhan pneumonia",
        "wuhan virus"
    ]

    ret = False
    title, abstract = "", ""
    if row is not None:
        title = row["title"].strip()
        abstract = row["abstract"].strip()

    for kw in keywords:
        if kw in title.lower() or kw in abstract.lower() or kw in text.lower():
            ret = True
            break

    return ret


def deduplicate(orig, new, incl_abs):
    # keep the longest data, order:
    # - longest abstract
    # - longest paper body
    # - data with more paragraphs
    # - else: first
    orig_lens = []
    new_lens = []
    if incl_abs:
        orig_lens.append(len(orig["abstracts"][0]))
        new_lens.append(len(new["abstracts"][0]))
    else:
        orig_lens.append(len(orig["papers"][0].abstract))
        new_lens.append(len(new["papers"][0].abstract))

    orig_lens.extend([len(orig["papers"][0].body), len(orig["paragraphs"])])
    new_lens.extend([len(new["papers"][0].body), len(new["paragraphs"])])
    for orig_l, new_l in zip(orig_lens, new_lens):
        if orig_l != new_l:
            ret = orig if orig_l > new_l else new
            break

    return ret


def process_paper_body(row, json_path, incl_abs):
    full_text = ""
    samples = []
    full_text_dict = json.load(open(json_path, "r"))
    body_text = full_text_dict["body_text"]
    if len(body_text):
        for part_idx, part in enumerate(body_text):
            part_text = part["text"].strip()
            if part_text != "":
                full_text += part_text + "\n"
                if filter_by_kwords(text=part_text):
                    samples.append(
                        paragraph_from_row(row, part_idx, part_text, incl_abs)
                    )

    return full_text.strip(), samples


def process_paper(base_dir, row, incl_abs):
    batch = defaultdict(list)
    full_text = ""
    paragraphs = []

    # print(f"Processing {cord_uid}")
    # prefer pmc files
    field = row.get("pmc_json_files", row.get("pdf_json_files", ""))
    for json_path in [pa for pa in field.split("; ") if pa.strip() != ""]:
        json_path = base_dir.joinpath(json_path)
        if not json_path.exists() or not json_path.is_file():
            # pdf file does not exist
            continue

        full_text, samples = process_paper_body(row, json_path, incl_abs)
        if full_text != "":
            paragraphs.extend(samples)
            # found, dont search for other versions of the paper
            break

    # else paper without file
    full_text = full_text.strip()
    if full_text != "" and filter_by_kwords(text=full_text):
        batch["papers"].append(paper_from_row(row, full_text, incl_abs))
        # paragraphs will be empty if paper without body
        batch["paragraphs"].extend(paragraphs)
    elif incl_abs and filter_by_kwords(row=row):
        # include article in "papers" and "paragraphs" indices,
        # even if it has no body text
        # another paper without body
        batch["papers"].append(paper_from_row(row, "", incl_abs))
        batch["paragraphs"].append(paragraph_from_row(row, 0, "", incl_abs))

    if not incl_abs:
        if row["abstract"].strip() != "" and filter_by_kwords(row=row):
            batch["abstracts"].append(abstract_from_row(row))
        # paper without abstracts

    return batch


def process_metadata(
    es_conn, base_dir, meta_path, incl_abs=False, batch_size=100
):
    base_dir = Path(base_dir)
    reader = csv.DictReader(open(meta_path, "r"))
    total = file_len(meta_path)
    batch = defaultdict(list)
    # duplicates are usually contiguous
    last_uid = None

    print(f"Processing metadata from: {meta_path}")
    for row in tqdm(reader, total=total, desc="Reading metadata"):
        row_data = process_paper(base_dir, row, incl_abs)
        uid = row["cord_uid"].strip()
        if last_uid is None:
            last_uid = {uid: row_data}
        elif uid in last_uid:
            last_uid[uid] = deduplicate(last_uid[uid], row_data, incl_abs)
        else:
            # change uid, next different paper
            # move to batch
            for key, value in last_uid[uid].items():
                batch[key].extend(value)

            # check if batch full
            if len(batch["papers"]) >= batch_size:
                bulk_save(es_conn, batch)
                batch = defaultdict(list)

            last_uid = {uid: row_data}


def parse_data_version(data_name):
    ret = None
    date = data_name.split("-")[1:]
    if len(date) == 3:
        ret = "-".join(date)
    return ret


def save_data_version(data_version):
    version = Version(version=data_version)
    version.save()
    print(f"Saved data version as: {version.version}")


def main(metadata, data_dir, address, port, incl_abs, batch_size):
    es = get_connection(address, port)
    init_index(incl_abs)

    data_dir = Path(data_dir)
    data_version = parse_data_version(data_dir.name)
    if data_version is None:
        print("Warning: Unable to get data version!\nNot saving to database")
    else:
        save_data_version(data_version)

    if metadata is None:
        metadata = data_dir.joinpath("metadata.csv")
    process_metadata(es, str(data_dir), metadata, incl_abs, batch_size)


if __name__ == "__main__":
    main(**vars(parse_flags()))
