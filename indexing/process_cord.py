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
from es.indexing import (
    init_index,
    Paper,
    Paper_with_abs,
    Paragraph,
    Paragraph_with_abs,
    Abstract,
)
from es.es_connector import get_connection


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


def process_paper(base_dir, row, incl_abs, batch):
    counts = defaultdict(int)
    full_text = ""
    paragraphs = []

    # print(f"Processing {cord_uid}")
    # prefer pmc files
    field = row.get("pmc_json_files", row.get("pdf_json_files", None))
    if field is not None:
        for json_path in field.split("; "):
            json_path = base_dir.joinpath(json_path)
            if not json_path.exists() or not json_path.is_file():
                counts["enoent_files"] += 1
                continue

            full_text_dict = json.load(open(json_path, "r"))
            body_text = full_text_dict["body_text"]
            if len(body_text):
                for part_idx, part in enumerate(body_text):
                    part_text = part["text"].strip()
                    if part_text != "":
                        full_text += part_text + "\n"
                        par_sample = paragraph_from_row(
                            row, part_idx, part_text, incl_abs
                        )
                        paragraphs.append(par_sample)

                # found, dont search for other versions of the paper
                break
    else:
        counts["papers_without_file"] += 1

    full_text = full_text.strip()
    if full_text != "":
        batch["papers"].append(paper_from_row(row, full_text, incl_abs))
    else:
        counts["papers_without_body"] += 1

    if not incl_abs:
        if row["abstract"].strip() == "":
            counts["papers_without_abstract"] += 1
        else:
            batch["abstracts"].append(abstract_from_row(row))

    if len(paragraphs):
        batch["paragraphs"].extend(paragraphs)

    return batch, counts


def process_metadata(
    es_conn, base_dir, meta_path, incl_abs=False, batch_size=100
):
    base_dir = Path(base_dir)
    reader = csv.DictReader(open(meta_path, "r"))
    total = file_len(meta_path)
    batch = defaultdict(list)
    counts = defaultdict(int)
    count_extras = ["papers", "paragraphs"]
    if incl_abs:
        count_extras += ["abstracts"]

    print(f"Processing metadata from: {meta_path}")
    for row in tqdm(reader, total=total, desc="Reading metadata"):
        batch, next_counts = process_paper(base_dir, row, incl_abs, batch)
        for key, val in next_counts.items():
            counts[key] += val

        if len(batch["papers"]) >= batch_size:
            for key in count_extras:
                counts[key] += len(batch[key])
            bulk_save(es_conn, batch)
            batch = defaultdict(list)

    for key, value in counts.items():
        key_name = key.split("_")
        key_name[0] = key_name[0].capitalize()
        key_name = " ".join(key_name)
        print("{}: {}".format(key_name, value))


def main(metadata, data_dir, address, port, incl_abs, batch_size):
    es = get_connection(address, port)
    init_index(incl_abs)
    if metadata is None:
        metadata = Path(data_dir).joinpath("metadata.csv")
    process_metadata(es, data_dir, metadata, incl_abs, batch_size)


if __name__ == "__main__":
    main(**vars(parse_flags()))
