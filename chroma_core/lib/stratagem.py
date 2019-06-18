import requests
import settings

from toolz.functoolz import pipe, partial
from requests.exceptions import ConnectionError

stratagem_measurement = "stratagem_scan"

size_distribution_name_table = {
    "size < 1m": "less_than_1m",
    "1m <= size < 1g": "greater_than_equal_1m_less_than_1g",
    "size >= 1g": "greater_than_equal_1g",
    "size >= 1t": "greater_than_equal_1t",
}


filter_out_other_counter = partial(filter, lambda counter: counter.get("name").lower() != "other")
flatten = lambda xs: [item for l in xs for item in l]
tuple_to_equals = lambda (a, b): "=".join((str(a), str(b)))

def create_stratagem_influx_point(tags, fields):
  return "{},{} {}".format(
    stratagem_measurement,
    ",".join(map(tuple_to_equals, tags)), 
    ",".join(map(tuple_to_equals, fields))
  )

def parse_size_distribution(counters):
    return pipe(
      counters,
      filter_out_other_counter,
      partial(map, lambda x: x.update({"name": size_distribution_name_table[x.get("name").lower()]}) or x),
      partial(
        map,
        lambda x: create_stratagem_influx_point(
          [("group_name", "size_distribution"), ("counter_name", x.get("name"))],
          [("count", x.get("count"))]
        )
      )
    )


def parse_user_distribution(counters):
    return pipe(
        counters,
        partial(filter, lambda x: "classify" in x),
        partial(map, lambda x: x.get("classify")),
        partial(map, lambda x: map(lambda y, x=x: y.update({"attr_type": x.get("attr_type")}) or y, x.get("counters"))),
        partial(flatten),
        partial(
          map,
          lambda x: create_stratagem_influx_point(
            [("group_name", "user_distribution"), ("classify_attr_type", x.get("attr_type")), ("counter_name", x.get("name"))],
            [("count", x.get("count"))]
          )
        )
    )

def parse_stratagem_results_to_influx(stratagem_results_json):
    parse_fns = {
      "size_distribution": partial(parse_size_distribution),
      "user_distribution": partial(parse_user_distribution)
    }

    group_counters = stratagem_results_json.get("group_counters")

    return pipe(
      [], 
      partial(
        reduce,
        lambda out, cur: out + [(cur.get("name"), cur.get("counters"))], 
        group_counters
      ), 
      partial(
        filter,
        lambda (key, val): key != "warn_purge_times"
      ),
      partial(
        map,
        lambda (key, val), parse_fns=parse_fns: parse_fns[key](val)
      ),
      partial(flatten)
    )

def clear_scan_results():
    response = requests.post(
      "http://{}:{}/query".format(settings.SERVER_FQDN, settings.INFLUXDB_PORT),
      params={
        "db": settings.INFLUXDB_STRATAGEM_SCAN_DB,
        "q":  "DELETE FROM {}".format(stratagem_measurement)
      }
    )

    response.raise_for_status()

def record_stratagem_point(point):
    response = requests.post(
      "http://{}:{}/write?db={}".format(settings.SERVER_FQDN, settings.INFLUXDB_PORT, settings.INFLUXDB_STRATAGEM_SCAN_DB),
      data=point
    )

    response.raise_for_status()
