import requests
import settings
import operator
import json

from toolz.functoolz import pipe, partial
from requests.exceptions import ConnectionError

temp_stratagem_measurement = "temp_stratagem_scan"
stratagem_measurement = "stratagem_scan"

size_distribution_name_table = {
    "size < 1m": "less_than_1m",
    "1m <= size < 1g": "greater_than_equal_1m_less_than_1g",
    "size >= 1g": "greater_than_equal_1g",
    "size >= 1t": "greater_than_equal_1t",
}


filter_out_other_counter = partial(filter, lambda counter: counter.get("name").lower() != "other")
flatten = lambda xs: [item for l in xs for item in l]


def tuple_to_equals(xs):
    return "{}={}".format(*xs)


def create_stratagem_influx_point(measurement, tags, fields, time):
    return "{},{} {} {}".format(
        measurement, ",".join(map(tuple_to_equals, tags)), ",".join(map(tuple_to_equals, fields)), time or ""
    ).rstrip()


def parse_size_distribution(measurement, counters):
    return pipe(
        counters,
        filter_out_other_counter,
        partial(map, lambda x: x.update({"name": size_distribution_name_table[x.get("name").lower()]}) or x),
        partial(
            map,
            lambda x: create_stratagem_influx_point(
                measurement,
                [("group_name", "size_distribution"), ("counter_name", x.get("name"))],
                [("count", x.get("count"))],
                None,
            ),
        ),
    )


def parse_user_distribution(measurement, counters):
    return pipe(
        counters,
        partial(filter, lambda x: "classify" in x),
        partial(map, lambda x: x.get("classify")),
        partial(map, lambda x: map(lambda y, x=x: y.update({"attr_type": x.get("attr_type")}) or y, x.get("counters"))),
        partial(flatten),
        partial(
            map,
            lambda x: create_stratagem_influx_point(
                measurement,
                [
                    ("group_name", "user_distribution"),
                    ("classify_attr_type", x.get("attr_type")),
                    ("counter_name", x.get("name")),
                ],
                [("count", x.get("count"))],
                None,
            ),
        ),
    )


def parse_stratagem_results_to_influx(measurement, stratagem_results_json):
    parse_fns = {
        "size_distribution": partial(parse_size_distribution, measurement),
        "user_distribution": partial(parse_user_distribution, measurement),
    }

    group_counters = stratagem_results_json.get("group_counters")

    return pipe(
        [],
        partial(reduce, lambda out, cur: out + [(cur.get("name"), cur.get("counters"))], group_counters),
        partial(filter, lambda xs: xs[0] not in ["warn_fids", "purge_fids"]),
        partial(map, lambda xs, parse_fns=parse_fns: parse_fns[xs[0]](xs[1])),
        partial(flatten),
    )


def clear_scan_results(clear_measurement_query):
    response = requests.post(
        "http://{}:8086/query".format(settings.SERVER_FQDN),
        params={"db": settings.INFLUXDB_STRATAGEM_SCAN_DB, "q": clear_measurement_query},
    )

    response.raise_for_status()


def record_stratagem_point(point):
    response = requests.post(
        "http://{}:8086/write?db={}".format(settings.SERVER_FQDN, settings.INFLUXDB_STRATAGEM_SCAN_DB), data=point
    )

    response.raise_for_status()


def aggregate_points(measurement_query):
    response = requests.get(
        "http://{}:8086/query".format(settings.SERVER_FQDN),
        params={"db": settings.INFLUXDB_STRATAGEM_SCAN_DB, "epoch": 0, "q": measurement_query},
    )

    results = json.loads(response._content).get("results")[0]
    values = results.get("series")[0].get("values")
    columns = results.get("series")[0].get("columns")

    points = map(lambda xs, columns=columns: pipe(zip(columns, xs), dict), values)

    counter_names = pipe(points, partial(map, lambda point: point.get("counter_name")), set, list)

    grouped_points = reduce(
        lambda agg, cname, points=points: agg + [filter(lambda point: point.get("counter_name") == cname, points)],
        counter_names,
        [],
    )

    sums = pipe(
        grouped_points,
        partial(map, lambda points: map(lambda point: point.get("count"), points)),
        partial(map, partial(reduce, operator.add)),
    )

    aggregated = pipe(
        grouped_points,
        partial(map, lambda xs: sorted(xs, key=lambda k: k["time"], reverse=True)),
        partial(map, lambda xs: xs[0]),
        partial(zip, sums),
        partial(map, lambda xs: xs[1].update({"count": xs[0]}) or xs[1]),
    )

    return aggregated


def join_entries_with_new_line(entries):
    return "\n".join(entries)


def submit_aggregated_data(measurement, aggregated):
    points = map(
        lambda point: (
            [
                ("counter_name", point.get("counter_name")),
                ("classify_attr_type", point.get("classify_attr_type")),
                ("group_name", point.get("group_name")),
            ],
            [("count", point.get("count"))],
            point.get("time"),
        ),
        aggregated,
    )

    return pipe(
        points,
        partial(map, lambda xs: create_stratagem_influx_point(measurement, xs[0], xs[1], xs[2])),
        join_entries_with_new_line,
        partial(record_stratagem_point),
    )
