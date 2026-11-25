"""Create (or update) the WikiHeatmap dashboard in Metabase through its API.

Idempotent: first run completes Metabase's setup (admin user) and adds the
PostgreSQL connection; every run creates or updates, by name, the collection,
the questions and the dashboard. Run after `docker compose up -d metabase`:

    uv run python scripts/setup_metabase.py

Env: MB_URL (default http://localhost:3000), MB_ADMIN_EMAIL, MB_ADMIN_PASSWORD,
PG_USER, PG_PASSWORD (the connection is made from the Metabase container,
so the host is the compose service name "postgres").
Standard library only, so it needs no extra dependency.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid

MB_URL = os.environ.get("MB_URL", "http://localhost:3000")
ADMIN_EMAIL = os.environ.get("MB_ADMIN_EMAIL", "admin@wikiheatmap.local")
ADMIN_PASSWORD = os.environ.get("MB_ADMIN_PASSWORD", "wikiheatmap-admin1")
PG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "wikiheatmap",
    "user": os.environ.get("PG_USER", "wikiheatmap"),
    "password": os.environ.get("PG_PASSWORD", "wikiheatmap-secret"),
}
COLLECTION = "WikiHeatmap"
DASHBOARD = "Trang nhiều người sửa nhất"
WINDOWS = [["1 hour", "1 giờ gần nhất"], ["day", "Ngày (UTC)"],
           ["week", "Tuần (thứ Hai, UTC)"], ["month", "Tháng (UTC)"]]

session = None


def api(method: str, path: str, body=None):
    req = urllib.request.Request(MB_URL + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    if session:
        req.add_header("X-Metabase-Session", session)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {path} -> {e.code}: {e.read().decode()[:500]}")


def wait_until_up():
    for _ in range(60):
        try:
            with urllib.request.urlopen(MB_URL + "/api/health", timeout=5) as resp:
                if json.loads(resp.read()).get("status") == "ok":
                    return
        except (urllib.error.URLError, ConnectionError, json.JSONDecodeError):
            pass
        time.sleep(5)
    sys.exit(f"Metabase not up at {MB_URL}")


def login():
    global session
    props = api("GET", "/api/session/properties")
    if props.get("setup-token") and not props.get("has-user-setup"):
        api("POST", "/api/setup", {
            "token": props["setup-token"],
            "user": {"first_name": "WikiHeatmap", "last_name": "Admin",
                     "email": ADMIN_EMAIL, "password": ADMIN_PASSWORD,
                     "site_name": "WikiHeatmap"},
            "prefs": {"site_name": "WikiHeatmap", "allow_tracking": False},
        })
        print(f"setup done, admin {ADMIN_EMAIL}")
    session = api("POST", "/api/session", {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})["id"]


def upsert_database() -> int:
    dbs = api("GET", "/api/database")
    dbs = dbs["data"] if isinstance(dbs, dict) else dbs
    body = {"engine": "postgres", "name": "WikiHeatmap", "details": {**PG, "ssl": False}}
    for db in dbs:
        if db["name"] == "WikiHeatmap":
            api("PUT", f"/api/database/{db['id']}", body)
            return db["id"]
    return api("POST", "/api/database", body)["id"]


def upsert_collection() -> int:
    for c in api("GET", "/api/collection"):
        if c.get("name") == COLLECTION and not c.get("archived"):
            return c["id"]
    return api("POST", "/api/collection", {"name": COLLECTION, "color": "#509EE3"})["id"]


def tags(*names):
    defs = {
        "window": {"type": "text", "display-name": "Window", "required": True, "default": "1 hour"},
        "date": {"type": "date", "display-name": "Ngày"},
        "wiki": {"type": "text", "display-name": "Wiki"},
    }
    return {n: {"id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"wikiheatmap/{n}")), "name": n, **defs[n]}
            for n in names}


# The period shown: the latest one of the chosen window, or the one that
# contains the chosen date. rankings has one period for the 1-hour window.
PERIOD = """
with period as (
    select max(period_start) as period_start
    from analytics.rankings
    where window_type = {{window}}
    [[and period_start <= {{date}}::timestamp at time zone 'UTC']]
)"""

RANKING_SQL = PERIOD + """
select
    row_number() over (order by r.editors desc, r.edits desc, r.wiki, r.title) as "#",
    r.wiki as "Wiki",
    r.title as "Trang",
    r.editors as "Số editor",
    r.edits as "Số edit",
    'https://' || coalesce(w.server_name, r.wiki) || '/wiki/' || replace(r.title, ' ', '_') as "Link",
    r.period_start as "Từ",
    r.period_end as "Đến",
    r.is_complete as "Kỳ đã trọn"
from analytics.rankings r
join period p on r.period_start = p.period_start
left join analytics.dim_wiki w on w.wiki = r.wiki
where r.window_type = {{window}}
[[and r.wiki = {{wiki}}]]
order by 1
limit 100
"""

TOP_CHART_SQL = PERIOD + """
select r.wiki || ': ' || r.title as "Trang", r.editors as "Số editor"
from analytics.rankings r
join period p on r.period_start = p.period_start
where r.window_type = {{window}}
[[and r.wiki = {{wiki}}]]
order by r.editors desc, r.edits desc, r.wiki, r.title
limit 15
"""

FRESHNESS_SQL = """
select 'Window 1 giờ (Spark Structured Streaming)' as "Nguồn",
       max(updated_at) as "Cập nhật lúc (UTC)"
from realtime.page_editors_1h
union all
select 'Ngày / tuần / tháng (dbt): edit mới nhất', max(event_time)
from analytics.fact_edits
"""


def upsert_card(db_id, coll_id, existing, name, sql, tag_names, display, viz=None) -> int:
    body = {
        "name": name,
        "collection_id": coll_id,
        "display": display,
        "visualization_settings": viz or {},
        "dataset_query": {"type": "native", "database": db_id,
                          "native": {"query": sql, "template-tags": tags(*tag_names)}},
    }
    if name in existing:
        api("PUT", f"/api/card/{existing[name]}", body)
        return existing[name]
    return api("POST", "/api/card", body)["id"]


def main():
    wait_until_up()
    login()
    db_id = upsert_database()
    coll_id = upsert_collection()
    items = api("GET", f"/api/collection/{coll_id}/items")["data"]
    cards = {i["name"]: i["id"] for i in items if i["model"] == "card"}
    dashboards = {i["name"]: i["id"] for i in items if i["model"] == "dashboard"}

    ranking = upsert_card(db_id, coll_id, cards, "Xếp hạng trang theo số editor", RANKING_SQL,
                          ["window", "date", "wiki"], "table",
                          {"column_settings": {'["name","Link"]': {"view_as": "link"}}})
    chart = upsert_card(db_id, coll_id, cards, "Top 15 trang", TOP_CHART_SQL,
                        ["window", "date", "wiki"], "row",
                        {"graph.dimensions": ["Trang"], "graph.metrics": ["Số editor"]})
    fresh = upsert_card(db_id, coll_id, cards, "Dữ liệu cập nhật tới", FRESHNESS_SQL, [], "table")

    # Wiki filter values: wikis with the most edits first.
    result = api("POST", "/api/dataset", {"type": "native", "database": db_id, "native": {
        "query": "select wiki from analytics.fact_edits group by wiki order by count(*) desc"}})
    wikis = [r[0] for r in result["data"]["rows"]]

    params = [
        {"id": "window", "name": "Window", "slug": "window", "type": "string/=",
         "default": ["1 hour"], "required": True,
         "values_query_type": "list", "values_source_type": "static-list",
         "values_source_config": {"values": WINDOWS}},
        {"id": "date", "name": "Ngày trong kỳ", "slug": "date", "type": "date/single"},
        {"id": "wiki", "name": "Wiki", "slug": "wiki", "type": "string/=",
         "values_query_type": "list", "values_source_type": "static-list",
         "values_source_config": {"values": wikis}},
    ]
    if DASHBOARD in dashboards:
        dash_id = dashboards[DASHBOARD]
    else:
        dash_id = api("POST", "/api/dashboard", {"name": DASHBOARD, "collection_id": coll_id})["id"]

    def mappings(card_id, names):
        return [{"parameter_id": n, "card_id": card_id, "target": ["variable", ["template-tag", n]]}
                for n in names]

    api("PUT", f"/api/dashboard/{dash_id}", {
        "name": DASHBOARD,
        "description": "Trang Wikipedia có nhiều editor khác nhau nhất (người, không tính bot), "
                       "theo window 1 giờ gần nhất, ngày, tuần hoặc tháng. Chỉ đếm, không diễn giải.",
        "parameters": params,
        "dashcards": [
            {"id": -1, "card_id": fresh, "row": 0, "col": 0, "size_x": 24, "size_y": 3,
             "parameter_mappings": []},
            {"id": -2, "card_id": chart, "row": 3, "col": 0, "size_x": 24, "size_y": 8,
             "parameter_mappings": mappings(chart, ["window", "date", "wiki"])},
            {"id": -3, "card_id": ranking, "row": 11, "col": 0, "size_x": 24, "size_y": 12,
             "parameter_mappings": mappings(ranking, ["window", "date", "wiki"])},
        ],
    })
    print(f"dashboard: {MB_URL}/dashboard/{dash_id}")


if __name__ == "__main__":
    main()
