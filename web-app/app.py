import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import pandas as pd

from transportation import process_transport_optimisation

# ---------------- Load data ----------------
results = process_transport_optimisation("./web-app/events-data.json")
venues = results["venues"]
peak_flows = results["peak_flows"]
recommendations = results["recommendations"]

# ---------------- Create app ----------------
app = dash.Dash(__name__)
app.title = "Festival Transport Insights"

# ---------------- Layout ----------------
app.layout = html.Div([

    # Navigation
    html.Nav([
        html.Div("Festival Ecosystem", className="nav-logo"),
        html.Ul([
            html.Li(html.A("Home", href="#home-section")),
            html.Li(html.A("Transport", href="#transport-section")),
            html.Li(html.A("Peak Flows", href="#peak-section")),
            html.Li(html.A("Recommendations", href="#rec-section")),
        ], className="nav-links")
    ], className="navbar"),

    html.Div(className="main-content", children=[

        # Home / Intro Section
        html.Section([
            html.H1("Festivals management made simply for organisers and businesses", className="title"),
            html.P("Insights on events, underserved zones and recommendations", className="slogan")
        ], id="home-section", className="section"),

        # Venues / Transport Section
        html.Section([
            html.H2("Venues & Underserved Zones"),
            html.Div("Shows computed UZI (underserved zone index) for each venue."),
            html.Div(id="venues-table-container")
        ], id="transport-section", className="section"),

        # Peak flows Section
        html.Section([
            html.H2("Peak Flows"),
            html.Div("Predicted ingress / egress for selected events."),
            html.Div(id="peak-flows-container")
        ], id="peak-section", className="section"),

        # Recommendations Section
        html.Section([
            html.H2("Recommendations"),
            html.Div([
                html.Label("Filter by level:"),
                dcc.Dropdown(
                    id="rec-filter-dropdown",
                    options=[
                        {"label": "Critical", "value": "critical"},
                        {"label": "High", "value": "high"},
                        {"label": "Medium", "value": "medium"},
                    ],
                    value=["critical"],
                    multi=True
                ),
                html.Div(id="rec-container", className="recommendation-wrapper")
            ])
        ], id="rec-section", className="section")
    ])
])

#  Callbacks 
@app.callback(
    Output("rec-container", "children"),
    Input("rec-filter-dropdown", "value")
)
def update_recommendations(selected_levels):
    # Filter recommendations by level
    df = recommendations[recommendations["rec_label"].str.lower().isin(selected_levels)]
    rec_items = []
    for _, row in df.iterrows():
        rec_items.append(
            html.Div([
                html.Div(f"Event: {row['event_name']}", style={"fontWeight": "bold"}),
                html.Div(f"Time: {row['ts']}"),
                html.Div(f"Venue: {row['place_name']} | UZI: {row['UZI_norm']:.2f}"),
                html.Div(f"Recommendation: {row['rec_message']}"),
                html.Div(f"Duration: {row['duration']}"),
                html.Div(f"Event ID: {row['event_id']}"),
            ], className=f"recommendation-card {row['rec_label'].lower()}")
        )
    return rec_items


if __name__ == "__main__":
    app.run()
