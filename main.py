import datetime
import os
import sqlite3
from itertools import chain, repeat
from typing import List, NamedTuple

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html
from dotenv import load_dotenv

load_dotenv()
JST = datetime.timezone(datetime.timedelta(hours=+9), "JST")
CALENDAR_DB_PATH = os.environ["CALENDAR_DB_PATH"]
TARGET_CATEGORIES = os.environ["TARGET_CATEGORIES"].split(",")


class DBData(NamedTuple):
    calendar_name: str
    summary: str
    start: float
    end: float


sqlite_conn = sqlite3.connect(CALENDAR_DB_PATH)
cur = sqlite_conn.cursor()
cur.execute(
    """
    SELECT
        calendar_name,
        summary,
        start_date + 978307200,
        end_date + 978307200
    FROM
        CalendarItem
    inner join
    (
        select
            ROWID as calendar_id,
            title as calendar_name
        from
            Calendar
    )
        using(calendar_id)
    order by
        end_date desc
"""
)
raw_result = cur.fetchall()
sqlite_conn.close()

result = [DBData(*r) for r in raw_result]


class Event(NamedTuple):
    start: datetime.datetime
    end: datetime.datetime
    duration: float
    category: str
    subcategory: str
    name: str


app = Dash(__name__)

data: List[Event] = []
for r in result:
    category = r.calendar_name
    if category not in TARGET_CATEGORIES:
        continue

    if ":" in r.summary:
        subcategory, text = r.summary.split(":")
    else:
        subcategory = r.summary
        text = r.summary

    start_time = datetime.datetime.fromtimestamp(r.start, tz=JST)
    end_time = datetime.datetime.fromtimestamp(r.end, tz=JST)
    duration = (end_time - start_time).total_seconds() / 3600

    data.append(
        Event(
            start=start_time,
            end=end_time,
            duration=duration,
            category=category,
            subcategory=subcategory,
            name=text,
        )
    )

df = pd.DataFrame(data)

conn = duckdb.connect()
category_duration_df = conn.execute(
    """
        select
            subcategory,
            sum(duration) as duration
        from
            df
        group by
            subcategory
        order by
            duration desc
    """
).df()
category_duration_fig = go.Figure()
category_duration_fig.add_trace(
    go.Bar(
        x=category_duration_df["subcategory"].tolist(),
        y=category_duration_df["duration"].tolist(),
        marker_color=px.colors.qualitative.Pastel1,
    )
)

yyyymm_duration_df = conn.execute(
    """
        select
            date_part('year', start) || right('0' || date_part('month', start), 2) as yyyymm,
            sum(duration) as duration
        from
            df
        group by
            yyyymm
        order by
            yyyymm
    """
).df()
yyyymm_duration_fig = go.Figure()
yyyymm_duration_fig.add_trace(
    go.Bar(
        x=yyyymm_duration_df["yyyymm"].tolist(),
        y=yyyymm_duration_df["duration"].tolist(),
        marker_color=px.colors.qualitative.Pastel1,
    )
)

subcategories_num = len(category_duration_df["subcategory"].tolist())
colors = list(
    chain.from_iterable(
        repeat(
            px.colors.qualitative.Pastel1,
            subcategories_num // len(px.colors.qualitative.Pastel1) + 1,
        )
    )
)

tree_color_map = {"(?)": "lightgrey"} | {
    subcategory: colors[i]
    for i, subcategory in enumerate(category_duration_df["subcategory"].tolist())
}
tree_fig = px.treemap(
    conn.execute(
        """
            select
                category,
                subcategory,
                date_part('year', start) || right('0' || date_part('month', start), 2) as yyyymm,
                sum(duration) as duration
            from
                df
            group by
                category, subcategory, yyyymm
        """
    ).df(),
    path=[px.Constant("all"), "yyyymm", "category", "subcategory"],
    color="subcategory",
    color_discrete_map=tree_color_map,
    values="duration",
    title="Time spent per category",
)
tree_fig.update_traces(marker=dict(cornerradius=5))

app.layout = html.Div(
    children=[
        dcc.Graph(id="graph-cagetory", figure=category_duration_fig),
        dcc.Graph(id="graph-yyyymm", figure=yyyymm_duration_fig),
        dcc.Graph(id="graph-tree", figure=tree_fig),
    ]
)


if __name__ == "__main__":
    app.run_server(debug=True)
