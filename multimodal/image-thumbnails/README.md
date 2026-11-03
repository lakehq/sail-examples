---
title: Make thumbnails from a folder of images
id: image-thumbnails
description: Use when you need to process a folder of image files (decode, resize, extract metadata) with Python in the engine, one row per file, without moving image bytes between stages.
tier: multimodal
level: intermediate
tags: [images, pillow, mapInArrow, files, thumbnails]
modality: image
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 120
needs_key: false
needs_gpu: false
prompt: Read every image in a folder, write a 64-pixel thumbnail for each one, and return a table with each image's format, size and mean color.
---

# Make thumbnails from a folder of images

This example creates 24 test images (PNG and JPEG, 320 to 1280 pixels wide). It writes a 64-pixel thumbnail for each image and returns a table of metadata. The image bytes stay inside the task that reads them.

## Run it

```bash
uv run main.py
```

## How it works

- **A table of paths.** The file list becomes a DataFrame with one `path` row per file, split into four partitions. Each task opens only the files in its own rows.
- **Decode and resize in `mapInArrow`.** Pillow opens each file, records its format and size, writes a thumbnail, and computes the thumbnail's mean color and checksum.
- **Small results out.** The function returns a few numbers per image, never the image bytes. Large binary values never flow between stages, so batches stay small.
- **Save once.** The metadata is written to Parquet before it is aggregated, so later queries never decode the images again.

The same pattern covers PDFs, audio and video: read the file in the UDF, return text, features or embeddings.

## Use your own files

Replace `make_images` and the `files` list with your own paths. They must be readable from wherever the UDF runs: local paths on one machine, object storage URLs (read with a client such as `fsspec`) on a cluster.
