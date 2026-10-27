# Compare the saved v1 seed with an explicit, fully paginated v2 API scan.
# Run from the repository root: python3 research/experiments/polymarket-protocol-adoption.py
import collections
import concurrent.futures
import time
import urllib.parse
import urllib.request
from decimal import Decimal
import datetime
import json
import sys
from pathlib import Path


class Reader:
    def __init__(self, stream):
        self.stream = stream
        self.buffer = ""
        self.eof = False
        self.decoder = json.JSONDecoder(parse_float=Decimal)

    def fill(self):
        chunk = self.stream.read(65536)
        self.buffer += chunk
        self.eof = not chunk

    def peek(self):
        self.buffer = self.buffer.lstrip()
        while not self.buffer and not self.eof:
            self.fill()
            self.buffer = self.buffer.lstrip()
        return self.buffer[:1]

    def expect(self, char):
        assert self.peek() == char, (char, self.buffer[:50])
        self.buffer = self.buffer[1:]

    def value(self):
        self.peek()
        while True:
            try:
                value, end = self.decoder.raw_decode(self.buffer)
                if end == len(self.buffer) and not self.eof:
                    self.fill()
                    continue
                self.buffer = self.buffer[end:]
                return value
            except json.JSONDecodeError:
                if self.eof:
                    raise
                self.fill()

    def members(self):
        self.expect("{")
        if self.peek() == "}":
            self.expect("}")
            return
        while True:
            key = self.value()
            assert isinstance(key, str)
            self.expect(":")
            yield key
            if self.peek() == "}":
                self.expect("}")
                return
            self.expect(",")



FLAGS = {"active": True, "closed": False, "archived": False,
         "acceptingOrders": True, "enableOrderBook": True}
FIELDS = ("id", "question", "version", "closed", "active", "archived",
          "acceptingOrders", "enableOrderBook", "volumeNum", "volume", "volume24hr",
          "createdAt", "acceptingOrdersTimestamp", "slug")

def initial():
    return {"markets": 0, "reported_volume24hr_usd": Decimal(0),
            "reported_lifetime_volume_usd": Decimal(0), "nonzero_volume24hr_markets": 0,
            "missing_volume24hr": 0, "missing_lifetime_volume": 0,
            "volume24hr_exceeds_lifetime": 0, "earliest_created_at": None}

def add(total, row):
    total["markets"] += 1
    life = row.get("volumeNum")
    if life is None: life = row.get("volume")
    day = row.get("volume24hr")
    for field, value, missing in (("reported_volume24hr_usd",day,"missing_volume24hr"),
                                  ("reported_lifetime_volume_usd",life,"missing_lifetime_volume")):
        if value is None:
            total[missing] += 1
        else:
            total[field] += Decimal(str(value))
    if day is not None and Decimal(str(day)) > 0: total["nonzero_volume24hr_markets"] += 1
    if day is not None and life is not None and Decimal(str(day)) > Decimal(str(life)):
        total["volume24hr_exceeds_lifetime"] += 1
    created = row.get("createdAt")
    if created and (total["earliest_created_at"] is None or created < total["earliest_created_at"]):
        total["earliest_created_at"] = created

def cached_seed(path):
    total = initial()
    versions = collections.Counter()
    prefix = {}
    with open(path) as stream:
        reader = Reader(stream)
        for key in reader.members():
            if key != "catalog":
                prefix[key] = reader.value()
                continue
            for section in reader.members():
                if section != "markets": raise RuntimeError("expected markets first")
                for market_id in reader.members():
                    market = reader.value()
                    row = market["metadata"]
                    assert market_id == row["id"]
                    assert all(row.get(k) is v for k,v in FLAGS.items())
                    versions[row.get("version")] += 1
                    add(total,row)
                return {"source": path, "seed":prefix, "versions":dict(versions), "tradable":total}
    raise RuntimeError("missing catalog")

def live_v2():
    result = {"source":"https://gamma-api.polymarket.com/markets/keyset", "version":"v2",
              "started_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "open":initial(), "tradable":initial(), "closed":initial(),
              "requests":0, "errors":0, "samples":[]}
    for closed in (False,True):
        seen = set(); cursors = set(); cursor = None
        while True:
            params=[("version","v2"),("closed",str(closed).lower()),("limit","100"),
                    ("order","id"),("ascending","true")]
            if cursor: params.append(("after_cursor",cursor))
            result["requests"] += 1
            with urllib.request.urlopen(result["source"]+"?"+urllib.parse.urlencode(params),timeout=30) as response:
                page=json.load(response,parse_float=Decimal)
            for row in page["markets"]:
                assert row["version"] == "v2" and row["closed"] is closed
                assert row["id"] not in seen
                seen.add(row["id"])
                add(result["closed" if closed else "open"],row)
                if all(row.get(k) is v for k,v in FLAGS.items()): add(result["tradable"],row)
                if len(result["samples"]) < 5:
                    result["samples"].append({k:row.get(k) for k in FIELDS})
            cursor=page.get("next_cursor")
            if not cursor: break
            assert cursor not in cursors
            cursors.add(cursor)
            time.sleep(0.1)
    result["completed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return result

if __name__ == "__main__":
    seed = sys.argv[1] if len(sys.argv)>1 else "data/markets.json"
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        cached=pool.submit(cached_seed,seed)
        live=pool.submit(live_v2)
        result={"v1_cached_seed":cached.result(),"v2_live":live.result(),
                "scope":"V1 figures cover tradable markets from the saved seed; v2 open and closed listings were fully traversed. Volume comparison uses tradable markets only, excludes closed markets, and is not total exchange 24h volume.",
                "volume_definition":"Sum of Gamma volume24hr and volumeNum (fallback volume), as reported; API aggregate statistics may lag or disagree."}
    output=Path("research/experiments/polymarket-protocol-adoption-results.json")
    output.write_text(json.dumps(result,default=str,indent=2)+"\n")
    print(json.dumps(result,default=str,indent=2),flush=True)
