---
title: Align robot camera frames with state and actions
id: align-robot-camera-frames
facts: [load-files-through-a-path-table, mapinarrow-arrow-types]
summary: Match robot camera frames to nearby state and action records.
description: Use when robot demonstrations store camera video separately from state/action records and you need sampled frames joined to nearby state timestamps with alignment errors and unmatched frames visible.
tier: multimodal
level: advanced
tags: [physical-ai, robotics, lerobot, video, timestamps, pyav, mapInArrow]
modality: video-state-action
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 120
needs_key: false
needs_gpu: false
prompt: Use LeRobot episode offsets to sample camera frames from a shared MP4, return small frame records from mapInArrow, and join each sample to the nearest state/action record within half a state period, exposing timestamp error and unmatched samples.
---

# Align robot camera frames with state and actions

Join sampled camera frames to the state and action records that accompany them.
This example uses the first three episodes of the simulated PushT task. The
episodes share one MP4, so episode metadata supplies the video offsets. The
output includes frame dimensions, a pixel checksum, the matched state/action
coordinates and the timestamp error.

## Run it

Run all commands below from the repository root (`sail-examples/`):

```bash
uv run multimodal/align-robot-camera-frames/main.py
```

The first run downloads about 7.7 MB into `multimodal/align-robot-camera-frames/.data/`. The dataset revision and every
downloaded file's SHA-256 are pinned. No key, model, GPU or robot is needed.

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3.

```text
+-------------+--------------+--------------+----------------+----------------------+
|episode_index|sampled_frames|matched_frames|unmatched_frames|max_time_error_seconds|
+-------------+--------------+--------------+----------------+----------------------+
|0            |17            |17            |0               |0.0                   |
|1            |12            |12            |0               |0.0                   |
|2            |15            |15            |0               |3.552713678800501E-15 |
+-------------+--------------+--------------+----------------+----------------------+

+-------------+------------+---------------+-----------------+-----+------+-----------+---------------+------------------+------------------+------------------+--------+--------+-------+
|episode_index|sample_index|video_timestamp|episode_timestamp|width|height|frame_index|state_timestamp|time_error_seconds|state_x           |state_y           |action_x|action_y|matched|
+-------------+------------+---------------+-----------------+-----+------+-----------+---------------+------------------+------------------+------------------+--------+--------+-------+
|0            |0           |0.0            |0.0              |96   |96    |0          |0.0            |0.0               |222.0             |97.0              |233.0   |71.0    |true   |
|0            |1           |1.0            |1.0              |96   |96    |10         |1.0            |0.0               |278.6406555175781 |114.0329360961914 |302.0   |137.0   |true   |
|0            |2           |2.0            |2.0              |96   |96    |20         |2.0            |0.0               |348.5765380859375 |190.74172973632812|360.0   |221.0   |true   |
|0            |3           |3.0            |3.0              |96   |96    |30         |3.0            |0.0               |372.2610778808594 |345.48870849609375|363.0   |384.0   |true   |
|0            |4           |4.0            |4.0              |96   |96    |40         |4.0            |0.0               |279.98089599609375|446.0788879394531 |262.0   |449.0   |true   |
|0            |5           |5.0            |5.0              |96   |96    |50         |5.0            |0.0               |256.5018615722656 |395.1748352050781 |259.0   |383.0   |true   |
|0            |6           |6.0            |6.0              |96   |96    |60         |6.0            |0.0               |266.73590087890625|332.57342529296875|268.0   |317.0   |true   |
|0            |7           |7.0            |7.0              |96   |96    |70         |7.0            |0.0               |262.0218200683594 |263.86041259765625|256.0   |257.0   |true   |
|0            |8           |8.0            |8.0              |96   |96    |80         |8.0            |0.0               |254.119140625     |216.7681884765625 |260.0   |191.0   |true   |
|0            |9           |9.0            |9.0              |96   |96    |90         |9.0            |0.0               |162.17274475097656|142.693115234375  |137.0   |158.0   |true   |
+-------------+------------+---------------+-----------------+-----+------+-----------+---------------+------------------+------------------+------------------+--------+--------+-------+
only showing top 10 rows

```

<!-- sail-output:end -->

## How it works

1. Read the state/action Parquet and the first three episodes' metadata.
2. Pass episode IDs and their video start/end offsets to `mapInArrow`. Each task
   fetches the pinned MP4, seeks to its episode and samples the first frame at or
   after each whole episode second. PyAV decodes the video on CPU.
3. Emit one small record per sample, including presentation time, dimensions and
   a SHA-256 of the decoded RGB pixels. Video bytes and pixel arrays stay inside
   the task. Save these records to temporary Parquet once, so downstream outputs
   reuse them without decoding again.
4. Subtract the episode's video offset to obtain episode-relative timestamps.
   Join to state rows in that episode within half a 10 Hz state period, plus
   0.0001 seconds for float timestamp rounding. Rank candidates by absolute time
   error, breaking ties with the earlier frame index.

`alignment` preserves samples with no qualifying state as unmatched rows, with
null state/action values. `coverage` reports sampled, matched and unmatched counts
and the largest matched timestamp error for each sampled episode. A decoding
failure fails the job; an episode that yields no sampled frames contributes no
rows. This is not a completeness check for all camera data in the dataset.

The half-period bound is an example matching rule for this synchronized dataset.
It does not estimate clock drift, interpolate state, calibrate sensors or prove
that independently recorded sensors are synchronized. A checksum identifies the
decoded pixels; it is not a visual quality score. The example prepares offline
data and does not control a robot.

See [curate robot episodes](../curate-robot-episodes/README.md) for structural
checks and a whole-episode selection manifest.

## Data and verification

The [PushT dataset](https://huggingface.co/datasets/lerobot/pusht), credited to the
[Diffusion Policy project](https://diffusion-policy.cs.columbia.edu/), declares
the MIT license in its dataset card. This example pins revision
`7628202a2180972f291ba1bc6723834921e72c19` and handles its one named video shard,
one camera and first three episodes.

Per the [LeRobot v3 format documentation](https://huggingface.co/docs/lerobot/en/lerobot-dataset-v3),
multiple episodes can share a video file. Use metadata offsets rather than
treating a file as an episode. The download cache is local to this example;
multi-machine use needs an appropriate worker-accessible cache or object store.

The independent reference decodes the beginning of the video sequentially,
without the worker sampler's seeks, and matches states with a Python scan.
The maintenance harness checks the returned rows, pixel checksums and coverage
against that reference.
