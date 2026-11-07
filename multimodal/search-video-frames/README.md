---
title: Find scenes in videos with text search
id: search-video-frames
description: Use when you need to search inside video files by description, by sampling frames, embedding them with CLIP (ONNX) inside the engine, splitting videos into scenes with SQL window functions, and ranking scenes per text query.
tier: multimodal
level: advanced
tags: [video, frames, clip, scene-detection, embeddings, vector-search, onnx, pyav, mapInArrow, window-functions]
modality: video
engines: both
ordered: true
float_tolerance: 1e-5
timeout: 240
needs_key: false
needs_gpu: false
prompt: Sample one frame per second from a folder of MP4 videos on Sail, embed the frames with the CLIP ViT-B/32 ONNX image encoder, split each video into scenes where consecutive frames stop looking alike, and return the two best-matching scenes with start and end times for each text query.
---

# Find scenes in videos with text search

This example searches inside video by description. The video is a 29-second montage of six real clips, cut together: a sunset at sea, a tram on a snowy street, a waterfall, a black cat, a rocket launch at night, and a hurricane seen from space. Inside the engine the example keeps one frame per second and embeds each frame with CLIP. SQL then splits the video into scenes and ranks them for queries such as "a black cat", returning each match with its start and end time.

## Run it

```bash
uv run main.py
```

The first run downloads the two CLIP ViT-B/32 encoders in ONNX format (about 600 MB together) from Hugging Face into the local Hugging Face cache. No account or key is needed. The model revisions are pinned.

The run finds all six scenes, and each query's best match is the scene that shows what the query describes. Times are whole seconds, because the example samples one frame per second: the rocket launch, which runs from 19.2 to 24.2 seconds in the video, comes back as 20 s to 25 s.

<!-- sail-output:start variant=default -->

## Example output

Recorded output · Sail 0.7.3.

The example has changed since this capture. This output comes from the earlier script.

```text
+------------------+-----+-----+---+------+-----------------+------------------------+
|             video|scene|start|end|frames|similarity_at_cut|lowest_similarity_within|
+------------------+-----+-----+---+------+-----------------+------------------------+
|scenes-montage.mp4|    1|    0|  5|     5|             NULL|                   0.949|
|scenes-montage.mp4|    2|    5| 10|     5|            0.498|                   0.922|
|scenes-montage.mp4|    3|   10| 15|     5|            0.465|                   0.977|
|scenes-montage.mp4|    4|   15| 20|     5|            0.538|                   0.925|
|scenes-montage.mp4|    5|   20| 25|     5|            0.408|                   0.968|
|scenes-montage.mp4|    6|   25| 30|     5|            0.635|                    0.98|
+------------------+-----+-----+---+------+-----------------+------------------------+

'a rocket launch at night' #1: scenes-montage.mp4 20s to 25s  score 0.290  (the scene shows: a rocket lifting off from its launch pad at night)
'a rocket launch at night' #2: scenes-montage.mp4 0s to 5s  score 0.215  (the scene shows: an orange sun low over a calm sea with a small boat)
'a black cat' #1: scenes-montage.mp4 15s to 20s  score 0.300  (the scene shows: a black cat lying on a light-colored carpet)
'a black cat' #2: scenes-montage.mp4 0s to 5s  score 0.210  (the scene shows: an orange sun low over a calm sea with a small boat)
'a waterfall' #1: scenes-montage.mp4 10s to 15s  score 0.291  (the scene shows: a waterfall pouring over a rock ledge into a pool)
'a waterfall' #2: scenes-montage.mp4 0s to 5s  score 0.204  (the scene shows: an orange sun low over a calm sea with a small boat)
'a tram on a snowy street' #1: scenes-montage.mp4 5s to 10s  score 0.332  (the scene shows: a tram approaching along tracks on a snowy city street)
'a tram on a snowy street' #2: scenes-montage.mp4 15s to 20s  score 0.129  (the scene shows: a black cat lying on a light-colored carpet)
```

<!-- sail-output:end -->

## How it works

- **A table of paths.** The videos in `videos/` are listed in a DataFrame with one `path` per file. Each task opens only the files in its own rows.
- **One video in, many rows out.** `mapInArrow` downloads the image encoder where the task runs (cached per machine) and loads it once per partition. For each video it decodes the frames with [PyAV](https://pyav.org) and keeps the first frame at or after each whole second, measured from the start of the video stream. It returns one row per kept frame: the video, the second and an `ARRAY<FLOAT>` vector. Frames are embedded and returned at most 32 at a time. This bounds frames per model call and output batch; memory also depends on frame resolution, model size and concurrent partitions.
- **Save once.** The frame vectors are written to Parquet and read back, so later queries never decode or embed the videos again.
- **Split into scenes in SQL.** `lag` over each video's sampled frames, in time order, gives the previous sampled frame's vector. When `vector_cosine_similarity` between consecutive sampled frames (one per second) drops below 0.8 (`SCENE_CHANGE_BELOW`), a new scene starts. A running `sum` of those starts numbers the scenes, and a `groupBy` gives each scene's start and end second. Each scene also reports `similarity_at_cut`, the similarity across the cut that starts it (null for a video's first scene), and `lowest_similarity_within`, the lowest similarity between consecutive sampled frames inside it.
- **Search in SQL.** Each query is embedded with CLIP's text encoder, every frame is scored with `vector_cosine_similarity`, and a scene's score is its best frame's score. A window function keeps the two best scenes per query.

`videos/scenes.csv` records where each clip really starts and ends. The output joins it in, so you can check the matches by eye.

## Use your own videos

Put your files in `videos/`, or build the `videos` DataFrame from any list of files, and change `QUERIES`. PyAV's wheels are linked against FFmpeg (per the PyAV README), so it opens the formats that FFmpeg build supports. The paths must be readable from wherever the UDF runs: local paths on one machine, object storage on a cluster (open the file with a client such as `fsspec` and pass the file object to `av.open`). A file that cannot be decoded raises an error and fails the job, and a video with no decodable frames returns no rows. To record such files and keep going, catch the error per file and return a status column, as the [OCR example](../ocr-scanned-pdfs/README.md) does.

On this video, the `scenes` output shows consecutive sampled frames (one per second) at least 0.92 similar within each scene, and at most 0.65 similar across each cut, so 0.8 separates them. Footage with fast camera motion or gradual transitions can fall between those values, so check the two similarity columns on a few of your own videos and adjust `SCENE_CHANGE_BELOW`. For long videos, keeping one frame every few seconds reduces the work per video.

## Video

Four clips are from Wikimedia Commons under CC0, and two are NASA videos. [videos/CREDITS.md](videos/CREDITS.md) lists each clip's source, creator and license.
