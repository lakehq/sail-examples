---
title: Search photos by description
id: search-images-by-text
facts: [vector-functions, cache-does-not-keep-results, udf-setup-per-partition]
summary: Find a photo by describing what you want to see.
featured: 2
preview: images/dog-snow.jpg
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

# Find a photo by describing it

You remember a dog in the snow, but not its filename. Give the example a sentence
and it ranks the photos that best match. It searches a small collection on your
laptop, without an API key or GPU.

First match for `a dog playing in the snow`:

![First match: a dog standing in snow](images/dog-snow.jpg)

First match for `a city street at night`:

![First match: a city street at night](images/street-night.jpg)

These are the first matches reported for the included collection. They illustrate
similarity, not exact understanding: the dog is standing in snow, even though the
query says "playing". [Photo sources and licenses](images/CREDITS.md).

## Run it

You need [uv](https://docs.astral.sh/uv/getting-started/installation/) and
[Git LFS](https://git-lfs.com). Run `git lfs pull` from the repository root (`sail-examples/`) to
fetch the photos if your checkout does not have them. Then run:

```bash
uv run multimodal/search-images-by-text/main.py
```

The first run installs
the packages and downloads about 600 MB of model weights from Hugging Face.
Later runs reuse the cached weights. No account is required.

The output lists the top three photos for each query and evaluates retrieval
against the collection's captions. For the included 53 photos, 52 captions find
their own photo first; all 53 find it in the top five. These results describe
this collection, not accuracy on every photo library.

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

## What is CLIP, and what does Sail do?

CLIP is a model that turns pictures and text into comparable lists of numbers,
called embeddings. A photo and a sentence with similar content should have
similar embeddings. This example runs CLIP through ONNX Runtime on the CPU.

Sail runs the Python functions that compute those embeddings, stores the photo
embeddings in Parquet, and ranks matches with SQL. CLIP supplies the image and
text representations; Sail handles the data workflow.

## The expression to keep

The script scores each query against each photo with:

```sql
vector_cosine_similarity(text_vec, image_vec)
```

Larger values rank earlier. The [source code](main.py) uses a window function to
keep three matches per query. It also measures how often each photo's caption
retrieves that photo.

## Try one change: search for a red car

[multimodal/search-images-by-text/main.py](main.py):

```diff main.py
 QUERIES = [
     "a dog playing in the snow",
     "a sailboat on the water",
     "a cat asleep",
     "a city street at night",
     "breakfast on a plate",
+    "a red car",
 ]
```

Rerun `uv run multimodal/search-images-by-text/main.py` from the repository root (`sail-examples/`). The output now includes three matches
for `a red car`; open those files in `multimodal/search-images-by-text/images/` to compare them.

## How it works

- **A table of paths.** `multimodal/search-images-by-text/images/captions.csv` lists each photo and its caption. The photos become a DataFrame with one `path` per file. Each task opens only the files in its own rows.
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
