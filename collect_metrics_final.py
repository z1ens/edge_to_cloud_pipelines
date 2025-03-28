
import requests
import pandas as pd

FLINK_REST_URL = "http://localhost:8081"

def get_job_id():
    response = requests.get(f"{FLINK_REST_URL}/jobs").json()
    jobs = response.get("jobs", [])
    for job in jobs:
        if job["status"] == "RUNNING":
            return job["id"]
    return None

def get_operators(job_id):
    url = f"{FLINK_REST_URL}/jobs/{job_id}/plan"
    response = requests.get(url).json()
    return [(node["id"], node["description"]) for node in response["plan"]["nodes"]]

def get_parallelism(job_id, operator_id):
    url = f"{FLINK_REST_URL}/jobs/{job_id}/vertices/{operator_id}"
    response = requests.get(url).json()
    return response.get("parallelism", 1)

def get_operator_ips(job_id, operator_id):
    url = f"{FLINK_REST_URL}/jobs/{job_id}/vertices/{operator_id}/subtasks"
    response = requests.get(url).json()
    return list(set([sub["host"] for sub in response.get("subtasks", [])]))

def get_metrics(job_id, operator_id, metric_name, aggregation="sum"):
    if metric_name == "end-to-end-latency":
        return None  # skip this metric

    parallelism = get_parallelism(job_id, operator_id)
    values = []

    for subtask_index in range(parallelism):
        full_metric_name = f"{subtask_index}.{metric_name}"
        url = f"{FLINK_REST_URL}/jobs/{job_id}/vertices/{operator_id}/metrics?get={full_metric_name}"
        response = requests.get(url).json()
        if isinstance(response, list) and response:
            try:
                values.append(float(response[0]["value"]))
            except (ValueError, TypeError):
                continue

    if not values:
        return None

    if aggregation == "sum":
        return sum(values)
    elif aggregation == "avg":
        return sum(values) / len(values)
    elif aggregation == "max":
        return max(values)
    else:
        return values  # raw list

def collect_all_metrics():
    job_id = get_job_id()
    if not job_id:
        print("No running job found.")
        return

    operators = get_operators(job_id)
    data = []

    for operator_id, description in operators:
        row = {
            "Operator": description,
            "OperatorID": operator_id,
            "Parallelism": get_parallelism(job_id, operator_id),
            "IPs": ", ".join(get_operator_ips(job_id, operator_id)),
            "numRecordsInPerSecond": get_metrics(job_id, operator_id, "numRecordsInPerSecond"),
            "numRecordsOutPerSecond": get_metrics(job_id, operator_id, "numRecordsOutPerSecond"),
            "busyTimeMsPerSecond": get_metrics(job_id, operator_id, "busyTimeMsPerSecond"),
            "idleTimeMsPerSecond": get_metrics(job_id, operator_id, "idleTimeMsPerSecond"),
            "mailboxLatencyMs_mean": get_metrics(job_id, operator_id, "mailboxLatencyMs.mean", aggregation="avg"),
            "backPressuredTimeMsPerSecond": get_metrics(job_id, operator_id, "backPressuredTimeMsPerSecond")
        }
        data.append(row)

    df = pd.DataFrame(data)

    # Calculate pipeline throughput if source and sink are present
    source_tput = df[df["Operator"].str.contains("Source", case=False)]["numRecordsOutPerSecond"].sum()
    sink_tput = df[df["Operator"].str.contains("Sink", case=False)]["numRecordsInPerSecond"].sum()
    df["EstimatedPipelineThroughput"] = min(source_tput, sink_tput)

    print(df)
    df.to_csv("flink_operator_metrics.csv", index=False)

if __name__ == "__main__":
    collect_all_metrics()
