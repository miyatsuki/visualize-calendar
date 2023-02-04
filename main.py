import datetime
from pathlib import Path
from typing import List, NamedTuple

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html

conn = duckdb.connect()


class Event(NamedTuple):
    start: datetime.datetime
    end: datetime.datetime
    duration: float
    category: str
    subcategory: str
    name: str


app = Dash(__name__)

data: List[Event] = []
for p in Path("ics").glob("*.ics"):
    category = p.stem

    with open(p) as f:
        lines = [line.strip() for line in f]

    for i, line in enumerate(lines):
        if line != "BEGIN:VEVENT":
            continue

        DTSTART = lines[i + 4].split(":")[1]
        DTEND = lines[i + 2].split(":")[1]
        _, subcategory, *_text = lines[i + 7].split(":")
        text = ":".join(_text)

        start_time = datetime.datetime.strptime(DTSTART, "%Y%m%dT%H%M%S")
        end_time = datetime.datetime.strptime(DTEND, "%Y%m%dT%H%M%S")
        duration = (end_time - start_time).total_seconds() / 3600

        data.append(
            Event(
                start=datetime.datetime.strptime(DTSTART, "%Y%m%dT%H%M%S"),
                end=datetime.datetime.strptime(DTEND, "%Y%m%dT%H%M%S"),
                duration=duration,
                category=category,
                subcategory=subcategory,
                name=text,
            )
        )

df = pd.DataFrame(data)

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

tree_color_map = {"(?)": "lightgrey"} | { subcategory: px.colors.qualitative.Pastel1[i] for i, subcategory in enumerate(category_duration_df["subcategory"].tolist())}
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
        html.H1(children="Hello Dash"),
        html.Div(
            children="""
        Dash: A web application framework for your data.
    """
        ),
        dcc.Graph(id="graph-cagetory", figure=category_duration_fig),
        dcc.Graph(id="graph-yyyymm", figure=yyyymm_duration_fig),
        dcc.Graph(id="graph-tree", figure=tree_fig),
    ]
)


if __name__ == "__main__":
    app.run_server(debug=True)
