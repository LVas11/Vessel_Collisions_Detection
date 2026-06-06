import os
import math
import argparse
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

matplotlib.use("Agg")

def parse_summary(summary_path):
    info = {}
    with open(summary_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if ":" in line:
                key, value = line.split(":", 1)
                info[key.strip()] = value.strip()
    return info


def plot_trajectories(output_dir):
    summary_path = os.path.join(output_dir, "collision_summary.txt")
    traj_path = os.path.join(output_dir, "trajectories.csv")

    # Checking if the output files exist
    if not os.path.exists(summary_path) or not os.path.exists(traj_path):
        raise FileNotFoundError(
            f"Expected {summary_path} and {traj_path} — run collision_detector.py first."
        )
    
    # Parsing summary info and load trajectories
    info = parse_summary(summary_path)
    df = pd.read_csv(traj_path, parse_dates=["ts"])
    df = df.sort_values(["MMSI", "ts"])

    mmsis = df["MMSI"].unique()

    coll_lat = float(info["Latitude"])
    coll_lon = float(info["Longitude"])
    coll_ts = pd.to_datetime(info["Timestamp"])

    plt.figure(figsize=(8, 6))

    for mmsi in mmsis:
        vessel_df = df[df["MMSI"] == mmsi]

        # First non-NA name for this vessel, else fall back to the MMSI
        if "Name" in vessel_df.columns and not vessel_df["Name"].isna().all():
            name = vessel_df["Name"].dropna().iloc[0]
        else:
            name = str(mmsi)

        # Track line and markers for start and end
        line, = plt.plot(
            vessel_df["Longitude"],
            vessel_df["Latitude"],
            marker="o",
            linewidth=1.5,
            markersize=3,
            label=f"{name} ({mmsi})"
        )
        color = line.get_color()

        plt.scatter(
            vessel_df.iloc[0]["Longitude"],
            vessel_df.iloc[0]["Latitude"],
            marker="s", s=60, color=color, zorder=5
        )
        plt.scatter(
            vessel_df.iloc[-1]["Longitude"],
            vessel_df.iloc[-1]["Latitude"],
            marker="x", s=70, color=color, zorder=5
        )

    # Marking collision point
    plt.scatter(
        coll_lon,
        coll_lat,
        marker="*",
        s=180,
        color="red",
        edgecolors="black",
        linewidths=0.5,
        label="Collision point",
        zorder=6
    )

    # Correcting aspect of teh graph
    plt.gca().set_aspect(1.0 / math.cos(math.radians(coll_lat)))

    # Labels
    plt.title(f"Vessel trajectories ±10 min around collision\n{coll_ts}")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    out_path = os.path.join(output_dir, "collision_trajectory.png")
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"Saved trajectory plot to {out_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="./output")
    args = parser.parse_args()

    plot_trajectories(args.output)


if __name__ == "__main__":
    main()