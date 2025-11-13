from datetime import datetime
from transportation import process_transport_optimisation
import pandas as pd

def pretty_print(df, name):
    print("\n======================================================")
    print(f"📌 {name.upper()} DATAFRAME")
    print("======================================================")
    if df.empty:
        print("No data available.")
    else:
        print(df.head(15))
        print(f"\nShape: {df.shape}")
    print("------------------------------------------------------")

def main():
    date_from = datetime(2025, 1, 1)
    date_to = datetime(2025, 9, 1)

    print("🚀 Running transport optimisation...\n")
    results = process_transport_optimisation(date_from, date_to)

    pretty_print(results["venues"], "venues (UZI + attendance + metadata)")
    pretty_print(results["peak_flows"], "peak_flows (inflow/outflow per 15 min)")
    pretty_print(results["recommendations"], "recommendations (critical/high/medium)")

if __name__ == "__main__":
    main()