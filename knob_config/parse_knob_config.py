import json


def get_knobs(path):
    f = open(path, 'r')
    knobs_json = json.load(f)
    return knobs_json
