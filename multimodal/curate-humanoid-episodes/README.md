---
title: Curate humanoid robot episodes with every camera frame
id: curate-humanoid-episodes
facts: [load-files-through-a-path-table, mapinarrow-arrow-types]
summary: Check humanoid robot recordings and align every camera frame.
description: Use when real-robot demonstrations with several cameras and many joints need episode-level quality checks, a whole-episode training split, and every camera frame of an episode decoded and matched to its state record.
tier: multimodal
level: advanced
tags: [physical-ai, robotics, lerobot, video, timestamps, data-quality, pyav, mapInArrow]
modality: video-state-action
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 180
needs_key: false
needs_gpu: false
prompt: Check every episode of a LeRobot humanoid dataset for frame counts, contiguous indices, 30 Hz timestamp cadence and finite 28-joint state and action vectors, select whole episodes for training and validation, then decode every frame of four camera videos for one episode in mapInArrow and match each frame to its nearest state record.
---

# Curate humanoid robot episodes with every camera frame

Prepare real-robot demonstrations for training. This example reads all 301
episodes of a Unitree G1 humanoid stacking three blocks: 28 joints (both arms and
both dexterous hands) recorded at 30 Hz, with four cameras. It checks every
episode, selects whole episodes for training and validation, then decodes every
frame of all four cameras for one episode (episode 279) and matches each frame to
the state record taken at the same moment.

## Run it

Run all commands below from the repository root (`sail-examples/`):

```bash
uv run multimodal/curate-humanoid-episodes/main.py
```

The first run downloads about 63 MB into `multimodal/curate-humanoid-episodes/.data/`: the state and action records of
every episode, plus their metadata. It then reads episode 279's 22 seconds from
each of the four camera videos. Each video file holds many episodes, so the run
seeks to that episode over HTTP instead of downloading the whole file. No key,
model, GPU or robot is needed.

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3.

```text
+----------+-----+
|     split|count|
+----------+-----+
|     train|  240|
|validation|   61|
+----------+-----+

+-------------+----+---------------+------+----------------+--------------+---------------+------+
|episode_index|task|expected_frames|frames|duplicate_frames|bad_timestamps|invalid_vectors|usable|
+-------------+----+---------------+------+----------------+--------------+---------------+------+
+-------------+----+---------------+------+----------------+--------------+---------------+------+

+---------------+--------------+--------------+----------------+----------------------+
|camera         |decoded_frames|matched_frames|distinct_records|max_time_error_seconds|
+---------------+--------------+--------------+----------------+----------------------+
|cam_left_high  |666           |666           |666             |8.900960324353946E-7  |
|cam_left_wrist |666           |666           |666             |8.900960324353946E-7  |
|cam_right_high |666           |666           |666             |8.900965440261643E-7  |
|cam_right_wrist|666           |666           |666             |8.900960892788135E-7  |
+---------------+--------------+--------------+----------------+----------------------+

+-------------+-------------+------------+-----------------------+-----------+----------------------+-------+
|camera       |episode_index|frame_number|episode_timestamp      |frame_index|time_error_seconds    |matched|
+-------------+-------------+------------+-----------------------+-----------+----------------------+-------+
|cam_left_high|279          |0           |-5.6843418860808015E-14|0          |5.6843418860808015E-14|true   |
|cam_left_high|279          |1           |0.03333333333330302    |1          |1.7384991224389523E-9 |true   |
|cam_left_high|279          |2           |0.06666666666666288    |2          |3.476941401459044E-9  |true   |
|cam_left_high|279          |3           |0.0999999999999659     |3          |1.4901502254360821E-9 |true   |
|cam_left_high|279          |4           |0.13333333333332575    |4          |6.953882802918088E-9  |true   |
+-------------+-------------+------------+-----------------------+-----------+----------------------+-------+
only showing top 5 rows

```

<!-- sail-output:end -->

## How it works

1. **Check every episode.** Window functions compare each record's timestamp with
   its frame index and with the previous record, at the declared 30 Hz.
   Aggregations count records, distinct and missing frame indices, and state or
   action vectors that are missing, the wrong length or not finite. A full join
   with the episode metadata keeps episodes whose records are missing entirely.
2. **Select whole episodes.** Only episodes that pass every check are selected.
   Episode IDs divisible by five go to validation and the others to training, so
   no episode has frames in both splits.
3. **Decode every frame.** A four-row table lists, for each camera, the video file
   that holds the episode and the episode's start and end time inside it.
   `mapInArrow` opens each video where the task runs, seeks to the episode and
   decodes every frame with PyAV on CPU. It returns one small row per frame:
   presentation time, size and a SHA-256 of the decoded RGB pixels. Video bytes
   and pixel arrays never leave the task. The decoded rows are saved to temporary
   Parquet once, so the outputs reuse them without decoding again.
4. **Match frames to records.** Each frame joins the episode's state records
   within half a record period (plus 0.0001 seconds for float32 timestamp
   rounding) and keeps the nearest one, breaking ties with the earlier record.

The outputs are:

- `quality`: one row per episode with expected and actual record counts,
  duplicate records, bad timestamps, invalid vectors and a `usable` flag.
- `selection`: the usable episodes with their `train` or `validation` split.
- `alignment`: one row per decoded frame with the matched record and the time
  error. A frame with no record in range is kept, with `matched` false.
- `coverage`: per camera, decoded frames, matched frames, distinct records matched
  and the largest time error.

At this revision all 301 episodes pass, the split is 240 training and 61
validation episodes, and all 666 frames of each camera match a different record
of episode 279's 666.

These checks concern record structure and timing. They do not judge task
success, demonstration quality or action safety, and the split is illustrative,
not the dataset's own. The half-period rule suits this synchronized dataset; it
does not estimate clock drift or calibrate cameras. A checksum identifies decoded
pixels; it is not an image quality score. Adapt the joint count, cadence and
episode choice for your own data.

For the same steps on a small simulated dataset, see
[curate robot episodes](../curate-robot-episodes/README.md) and
[align robot camera frames](../align-robot-camera-frames/README.md).

## Data and verification

The [G1 Dex3 block stacking dataset](https://huggingface.co/datasets/unitreerobotics/G1_Dex3_BlockStacking_Dataset)
is published by Unitree Robotics under the Apache 2.0 license. This example pins
revision `57faa2cf516e008f96d91fe3b67ad53a74f012e6`. It uses the LeRobot v3
layout ([format documentation](https://huggingface.co/docs/lerobot/en/lerobot-dataset-v3)),
in which episode metadata locates each episode inside shared Parquet and video
files.

Every downloaded file is checked against a pinned SHA-256, including cached
copies. The four camera videos are 200 to 500 MB each and are read by seeking, not
downloaded, so instead of hashing whole files the example pins a digest of every
decoded frame of each camera and fails if the pixels differ. RGB conversion uses
FFmpeg's bit-exact mode with accurate rounding so CPU acceleration does not change
the checked pixels. Files are downloaded locally and the videos are read
over HTTPS where each task runs; a multi-machine deployment needs the same
network access, or storage reachable from its workers.
