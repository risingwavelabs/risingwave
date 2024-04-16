import json
import csv
from io import StringIO


def gen_data(file_num, item_num_per_file):
    assert (
        item_num_per_file % 2 == 0
    ), f"item_num_per_file should be even to ensure sum(mark) == 0: {item_num_per_file}"
    return [
        [
            {
                "id": file_id * item_num_per_file + item_id,
                "name": f"{file_id}_{item_id}",
                "sex": item_id % 2,
                "mark": (-1) ** (item_id % 2),
            }
            for item_id in range(item_num_per_file)
        ]
        for file_id in range(file_num)
    ]


def format_json(data):
    return ["\n".join([json.dumps(item) for item in file]) for file in data]


def format_csv(data, with_header):
    csv_files = []

    for file_data in data:
        ostream = StringIO()
        writer = csv.DictWriter(ostream, fieldnames=file_data[0].keys())
        if with_header:
            writer.writeheader()
        for item_data in file_data:
            writer.writerow(item_data)
        csv_files.append(ostream.getvalue())
    return csv_files
