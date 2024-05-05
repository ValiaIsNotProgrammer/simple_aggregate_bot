
"""
Необходимо посчитать суммы всех выплат с {28.02.2022} по {31.03.2022}, единица группировки - {день}
"""
import datetime
import json
from collections import defaultdict
from typing import Dict, List

import bson

def __get_dict_from(path="dump/sampleDB/sample_collection.bson"):
    with open(path, 'rb') as f:
        data = bson.decode_all(f.read())
    return data


def __get_meta(text_msg: str) -> tuple[str, str, str]:
    json_data = json.loads(text_msg)
    dt_from, dt_upto, group_type = json_data["dt_from"], json_data["dt_upto"], json_data["group_type"]
    return dt_from, dt_upto, group_type

def aggregate(text_msg: str) -> dict[str, list[int] | list[str]]:
    """
    :param group_type: hour, day, week, month
    :type group_type:
    :return:
    """
    bson_data = __get_dict_from()
    dt_from, dt_upto, group_type = __get_meta(text_msg)

    dt_from = datetime.datetime.fromisoformat(dt_from)
    dt_upto = datetime.datetime.fromisoformat(dt_upto)

    dataset = []
    labels = []

    aggregated_data = defaultdict(int)

    filtered_data = [entry for entry in bson_data if dt_from <= entry['dt'] <= dt_upto]

    all_dates = set()
    current_date = dt_from

    while current_date <= dt_upto:
        all_dates.add(current_date)
        if group_type == 'hour':
            current_date += datetime.timedelta(hours=1)
        elif group_type == 'day':
            current_date += datetime.timedelta(days=1)
        elif group_type == 'week':
            current_date += datetime.timedelta(weeks=1)
        elif group_type == 'month':
            current_date = current_date.replace(day=1)
            current_date += datetime.timedelta(days=32)
            current_date = current_date.replace(day=1)
        else:
            raise ValueError("Invalid group_type")

    for entry in filtered_data:
        if group_type == 'hour':
            group_key = entry['dt'].replace(minute=0, second=0, microsecond=0)
        elif group_type == 'day':
            group_key = entry['dt'].replace(hour=0, minute=0, second=0, microsecond=0)
        elif group_type == 'month':
            group_key = entry['dt'].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            raise ValueError("Invalid group_type")

        aggregated_data[group_key] += entry['value']

    for date in sorted(all_dates):
        labels.append(date.isoformat())
        dataset.append(aggregated_data.get(date, 0))

    return {"dataset": dataset, "labels": labels}


# def test(jsons: list[dict], outputs: list[dict]) -> None:
#     for number, json in enumerate(jsons, start=1):
#         r = aggregate(json["dt_from"], json["dt_upto"], json["group_type"], data)
