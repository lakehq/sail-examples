---
title: Align robot camera frames with state and actions
id: align-robot-camera-frames
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

```bash
uv run main.py
```

The first run downloads about 7.7 MB into `.data/`. The dataset revision and every
downloaded file's SHA-256 are pinned. No key, model, GPU or robot is needed.

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
