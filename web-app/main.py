from transportation import (
    load_events_from_json,
    estimate_attendance,
    TransportAPIClient,
    compute_undserved_zone_index,
    process_transport_optimisation,
    generate_recommendations,
    INGRESS_BUFFER_MINUTES,
    EGRESS_BUFFER_MINUTES
)

if __name__ == "__main__":
    results = process_transport_optimisation("./web-app/events-data.json")
    print("Venues:")
    print(results["venues"])
    print("\nPeak flows:")
    print(results["peak_flows"].head())
    print("\nRecommendations:")
    print(results["recommendations"])
