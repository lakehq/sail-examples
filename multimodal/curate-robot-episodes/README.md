---
title: Curate robot demonstration episodes
id: curate-robot-episodes
description: Use when you need to check robot demonstration records for missing frames, duplicate frames, timestamp gaps and invalid state/action vectors before selecting whole episodes for training and validation.
tier: multimodal
level: intermediate
tags: [physical-ai, robotics, lerobot, parquet, data-quality, window-functions]
modality: state-action
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 120
needs_key: false
needs_gpu: false
prompt: Read LeRobot state/action Parquet and episode metadata, check lengths, contiguous frame indices, timestamp cadence and finite two-coordinate vectors, then return a quality report and a deterministic whole-episode train/validation selection manifest.
---

# Curate robot demonstration episodes

Check demonstrations before choosing training data. This example reads the
PushT dataset, a simulated task in which a circular agent pushes a T-shaped block
toward a target. It checks all 206 episodes and returns a quality report plus
a manifest assigning usable episodes to training or validation.

## Run it

```bash
uv run main.py
```

The first run downloads about 786 KB of Parquet and metadata into `.data/`.
No account, key, model, GPU or robot is needed. The dataset revision and file
SHA-256s are pinned; a checksum mismatch fails the run.

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3.

```text
quality
+-------------+-------------------------------------------------+---------------+------+----------------+--------------+---------------+------+
|episode_index|task                                             |expected_frames|frames|duplicate_frames|bad_timestamps|invalid_vectors|usable|
+-------------+-------------------------------------------------+---------------+------+----------------+--------------+---------------+------+
|0            |Push the T-shaped block onto the T-shaped target.|161            |161   |0               |0             |0              |true  |
|1            |Push the T-shaped block onto the T-shaped target.|118            |118   |0               |0             |0              |true  |
|2            |Push the T-shaped block onto the T-shaped target.|141            |141   |0               |0             |0              |true  |
|3            |Push the T-shaped block onto the T-shaped target.|159            |159   |0               |0             |0              |true  |
|4            |Push the T-shaped block onto the T-shaped target.|159            |159   |0               |0             |0              |true  |
|5            |Push the T-shaped block onto the T-shaped target.|157            |157   |0               |0             |0              |true  |
|6            |Push the T-shaped block onto the T-shaped target.|69             |69    |0               |0             |0              |true  |
|7            |Push the T-shaped block onto the T-shaped target.|169            |169   |0               |0             |0              |true  |
|8            |Push the T-shaped block onto the T-shaped target.|80             |80    |0               |0             |0              |true  |
|9            |Push the T-shaped block onto the T-shaped target.|134            |134   |0               |0             |0              |true  |
+-------------+-------------------------------------------------+---------------+------+----------------+--------------+---------------+------+
only showing top 10 rows

selection
+-------------+-------------------------------------------------+------+----------+
|episode_index|task                                             |frames|split     |
+-------------+-------------------------------------------------+------+----------+
|0            |Push the T-shaped block onto the T-shaped target.|161   |validation|
|1            |Push the T-shaped block onto the T-shaped target.|118   |train     |
|2            |Push the T-shaped block onto the T-shaped target.|141   |train     |
|3            |Push the T-shaped block onto the T-shaped target.|159   |train     |
|4            |Push the T-shaped block onto the T-shaped target.|159   |train     |
|5            |Push the T-shaped block onto the T-shaped target.|157   |validation|
|6            |Push the T-shaped block onto the T-shaped target.|69    |train     |
|7            |Push the T-shaped block onto the T-shaped target.|169   |train     |
|8            |Push the T-shaped block onto the T-shaped target.|80    |train     |
|9            |Push the T-shaped block onto the T-shaped target.|134   |train     |
+-------------+-------------------------------------------------+------+----------+
only showing top 10 rows

```

<!-- sail-output:end -->

## How it works

The frame table contains episode IDs, frame indices, timestamps and two-coordinate
state/action vectors. The episode metadata declares how many frames belong to
each demonstration. A full join preserves episodes whose records are entirely
missing and records whose episode IDs are absent from the metadata.

SQL window functions check that timestamps follow the declared 10 Hz cadence.
Aggregations check frame counts, unique indices, zero-based endpoints, finite
two-element vectors and a single known task. Timestamp checks allow 0.0001 seconds
for this dataset's float32 representation. `quality` exposes the counts and a
`usable` flag. `selection` includes only episodes that pass these checks.

The split assigns episode IDs divisible by five to validation and the others to
training. Keeping each episode together avoids putting frames from that episode
in both splits. This is an illustrative split, not the dataset's official split
or proof that demonstrations from related sessions or scenes are independent.
The output is a selection manifest, not rewritten training data.

These checks concern record structure and timing. They do not judge task success,
demonstration skill, action safety or suitability for a particular policy.
Adapt vector shapes, cadence and selection rules for your own task.

For the camera side of the same dataset, see
[align robot camera frames](../align-robot-camera-frames/README.md).

## Data and verification

The [PushT dataset](https://huggingface.co/datasets/lerobot/pusht) is credited to the
[Diffusion Policy project](https://diffusion-policy.cs.columbia.edu/); its dataset
card declares the MIT license. This example pins revision
`7628202a2180972f291ba1bc6723834921e72c19`. It uses the actual v3 metadata files;
the dataset card's embedded layout describes an older version.

Per the [LeRobot v3 format documentation](https://huggingface.co/docs/lerobot/en/lerobot-dataset-v3),
episode metadata identifies records inside shared Parquet and video shards.
This example reads one named Parquet shard and the corresponding metadata, not
an arbitrary LeRobot dataset layout. Files are downloaded locally before Sail
reads them. A multi-machine deployment needs storage accessible to its workers.

An independent Python reference reconstructs the report and selection from the
source Parquet. The maintenance harness checks every returned row against it.
