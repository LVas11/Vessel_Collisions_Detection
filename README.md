# Vessel Collision Detection

Repository for the AIS Vessel Collision Detection assignment.

The solution uses:

- Python 3.11
- Apache Spark (PySpark)
- Docker
- Pandas
- Matplotlib

## Repository Structure

```
ais_collision/
│
├── src/
│   ├── collision_detector.py
│   └── visualize.py
│
├── Dockerfile
├── docker-compose.yml
├── entrypoint.sh
├── requirements.txt
├── Report.md
│
├── output/
│   └── results
│
└── Data/
    └── AIS CSV files (not included)
```

## Files

- `collision_detector.py` - main code for collision detection
- `visualize.py` - code for creating the trajectory plot
- `Dockerfile` - setup for Docker image creation
- `entrypoint.sh` - sets how the two Python scripts are executed
- `requirements.txt` - dependencies required for the Python code
- `docker-compose.yml` - configuration file used to build and run the Docker container
- `Report.md` - final report with explanations of methodology

## Input Data

Data used: Danish AIS Data (http://aisdata.ais.dk/) for December 2021.

The solution expects Danish AIS CSV files placed in a local directory named `Data/`, which is mounted into the container at `/data`.

Example:

```
Data/
├── aisdk-2021-12-12.csv
├── aisdk-2021-12-13.csv
├── aisdk-2021-12-14.csv
└── ...
```

The data is not pushed to the repository due to its size.

## Building the Docker Image

```bash
docker compose build
```

## Running

Process all AIS files in `Data/`:

```bash
docker compose run --rm ais-detector
```

Process a specific file:

```bash
docker compose run --rm -e DATA_GLOB="/data/aisdk-2021-12-*.csv" ais-detector
```

## Submitted Docker Image

A compiled Docker image is provided as `ais-collision-detector.tar`.

Load it with:

```bash
docker load -i ais-collision-detector.tar
```

## Output Files

The container writes the following to `output/`:

- `collision_summary.txt` - summary of the detected collision (included in the report)
- `top_encounters.csv` - top 10 closest encounters by distance between ships
- `trajectories.csv` - data used to create the trajectory plot
- `collision_trajectory.png` - plot of the two vessels' trajectories within 10 minutes before and after the collision

## Result

The detected event is the documented *Karin Høj* / *Scot Carrier* collision of 13 December 2021 - see `output/collision_summary.txt` and the Report for details.
