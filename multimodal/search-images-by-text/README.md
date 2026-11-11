---
title: Search photos by text with CLIP
id: search-images-by-text
description: Use when you want to find photos that match a plain-language description, by embedding images and text with CLIP (ONNX) inside the engine and ranking them with SQL vector functions, and to measure retrieval quality in SQL.
tier: multimodal
level: intermediate
tags: [images, clip, text-to-image, embeddings, vector-search, onnx, mapInArrow, evaluation]
modality: image
engines: both
ordered: true
float_tolerance: 1e-5
timeout: 240
needs_key: false
needs_gpu: false
prompt: Embed a folder of photos and a list of text queries with the CLIP ViT-B/32 ONNX encoders on Sail, return the three best-matching photos for each query, and compute recall at 1 and at 5 using a written caption for each photo.
---

# Search photos by text with CLIP

CLIP has an image encoder and a text encoder that map pictures and sentences into the same vector space, so a sentence lands near the photos it describes. This example embeds 53 photos and a few plain-language queries inside the engine, and ranks the photos for each query with `vector_cosine_similarity`. It also scores the search: each photo has a written caption, and SQL counts how often the caption finds its own photo.

## Run it

```bash
uv run main.py
```

The first run downloads the two CLIP ViT-B/32 encoders in ONNX format (about 600 MB together) from Hugging Face into the local Hugging Face cache. No account or key is needed. The model revisions are pinned.

"a dog playing in the snow" ranks the dog in the snow first, and "a city street at night" ranks the night street first. Of the 53 captions, 52 rank their own photo first, and all 53 rank it in the top five.

<!-- sail-output:start variant=default -->

## Example output

Recorded output · Sail 0.7.3.

The example has changed since this capture. This output comes from the earlier script.

```text
a dog playing in the snow
  1. 0.279  dog-snow.jpg
  2. 0.254  fox.jpg
  3. 0.225  snow-trees.jpg
a sailboat on the water
  1. 0.268  sailboat-bay.jpg
  2. 0.254  sailboat.jpg
  3. 0.243  beach-tropical.jpg
a cat asleep
  1. 0.303  cat-sleeping-orange.jpg
  2. 0.295  cat-sleeping-curled.jpg
  3. 0.259  cat-tabby.jpg
a city street at night
  1. 0.307  street-night.jpg
  2. 0.234  bridge-night.jpg
  3. 0.196  bicycle-snow-night.jpg
breakfast on a plate
  1. 0.283  food-english-breakfast.jpg
  2. 0.265  food-pizza.jpg
  3. 0.242  food-caesar-salad.jpg
+--------+------------------+-----------+
|captions|       recall_at_1|recall_at_5|
+--------+------------------+-----------+
|      53|0.9811320754716981|        1.0|
+--------+------------------+-----------+

```

<!-- sail-output:end -->

## How it works

- **A table of paths.** `images/captions.csv` lists each photo and its caption. The photos become a DataFrame with one `path` per file. Each task opens only the files in its own rows.
- **Embed the photos.** `mapInArrow` downloads the image encoder where the task runs (cached per machine) and loads it once per partition. It applies CLIP's preprocessing: the shorter side is resized to 224 pixels, center-cropped, and normalized. A check against the transformers 4.57.1 CLIP image processor gave the same pixels. Each model call handles at most 16 photos. The function returns one vector per photo, scaled to length 1 and stored as `ARRAY<FLOAT>`, the type the vector functions accept.
- **Save once.** The photo vectors are written to Parquet and read back, so the queries never run the image encoder again.
- **Embed the text.** A second `mapInArrow` runs the text encoder. Model calls handle at most 16 queries, each padded to CLIP's fixed length of 77 tokens, so its vector does not depend on which other queries share its batch.
- **Search in SQL.** A cross join of queries and photos is scored with `vector_cosine_similarity`, and a window function ranks the photos per query.
- **Measure in SQL.** For each caption query, its recorded target filename gives recall at 1 and at 5. Repeated captions remain separate queries with their own targets.

The cross join scores every query against every photo. Check that this query-photo product fits your memory and runtime limits before increasing the gallery size. This example does not build or query a vector index.

## Use your own photos

Point `IMAGES` at your own folder with a `captions.csv`, or build the `photos` DataFrame from any list of files, and change `QUERIES`. The paths must be readable from wherever the UDF runs: local paths on one machine, object storage URLs (read with a client such as `fsspec`) on a cluster. Without captions, drop the recall query. A file that cannot be decoded raises an error and fails the job. To record such files and keep going, catch the error per file and return a status column, as the [OCR example](../ocr-scanned-pdfs/README.md) does.

## Photos

All 53 photos are from Wikimedia Commons. Each is either dedicated to the public domain under CC0, or in the public domain as a work of the US federal government; two of the government photos also carry a CC BY 2.0 tag. Each was resized to at most 512 pixels on its longer side. [images/CREDITS.md](images/CREDITS.md) lists every photo's source page, creator and license.
