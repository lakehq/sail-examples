---
title: Classify images with an ONNX model
id: classify-images
description: Use when you need to label a folder of images with a pretrained image classifier (batch inference in ONNX Runtime) inside the engine, and send low-confidence predictions to review.
tier: multimodal
level: intermediate
tags: [images, classification, onnx, imagenet, resnet, batch-inference, mapInArrow]
modality: image
engines: both
ordered: true
float_tolerance: 1e-5
timeout: 180
needs_key: false
needs_gpu: false
prompt: Classify every photo in a folder with the ResNet-18 ImageNet model in ONNX format on Sail, return the top three labels and scores per photo, and list the predictions below 0.6 confidence for review.
---

# Classify images with an ONNX model

This example labels eight photos with ResNet-18, an image classifier trained on the 1,000 ImageNet categories. It runs the model inside the engine with ONNX Runtime, returns the top three labels per photo, and lists the less confident predictions with the runner-up label, for a person to check.

## Run it

```bash
uv run main.py
```

The first run downloads the model (about 47 MB) from Hugging Face into the local Hugging Face cache. No account or key is needed. The model revision is pinned, so every run uses the same weights.

For these eight bundled photos, every top label matches the photo's subject. Five of them score 0.6 or more. The golden retriever, the Siamese cat and the tabby cat score less, so they land in the review table. For the Siamese cat, the runner-up (Persian cat) is close behind.

## How it works

- **A table of paths.** The image files become a DataFrame with one `path` row per file, split into two partitions. Each task opens only the files in its own rows.
- **Load once per partition.** `mapInArrow` downloads the model where the task runs (cached per machine), then opens the ONNX Runtime session before its loop over batches.
- **Preprocess like the model was trained.** Each image is resized so its shorter side is 256 pixels, center-cropped to 224 by 224, and normalized with the ImageNet mean and standard deviation. This follows the model's `preprocessor_config.json` (checked to give the same pixels as the transformers 4.57.1 image processor on these photos).
- **Batch inference.** Each Arrow batch is split into groups of at most 16 images per model call. A softmax turns the scores into probabilities, and the function returns the best label, its probability, and the top three as an `ARRAY<STRUCT<label, score>>`.
- **Save once.** The predictions are written to Parquet and read back, so the review query never runs the model again.
- **Review in SQL.** Predictions below 0.6 are selected with their runner-up label, taken from the `top3` array.

## Use your own images

Point `IMAGES` at your own folder, or build the `paths` table from any list of files. The paths must be readable from wherever the UDF runs: local paths on one machine, object storage URLs (read with a client such as `fsspec`) on a cluster. A file that cannot be decoded raises an error and fails the job. To record such files and keep going, catch the error per file and return a status column, as the [OCR example](../ocr-scanned-pdfs/README.md) does.

Evaluate the 0.6 cutoff on labeled images for your task before using it to route predictions for review.

To use another classifier, change `MODEL_REPO` and `MODEL_REVISION`, and match `preprocess` to the model's preprocessing settings (`preprocessor_config.json` in its repository).

## Photos

All eight photos are from Wikimedia Commons and dedicated to the public domain under CC0. They were resized to at most 640 pixels on the longer side.

| File | Source |
|---|---|
| `espresso.jpg` | [Antoccino Espresso](https://commons.wikimedia.org/wiki/File:Antoccino_Espresso.jpg) |
| `golden-retriever.jpg` | [Golden retriever 2](https://commons.wikimedia.org/wiki/File:Golden_retriever_2.jpg) |
| `monarch-butterfly.jpg` | [Danaus plexippus on a Zinnia](https://commons.wikimedia.org/wiki/File:Danaus_plexippus_on_a_Zinnia.jpg) |
| `red-fox.jpg` | [Vulpes vulpes ssp fulvus](https://commons.wikimedia.org/wiki/File:Vulpes_vulpes_ssp_fulvus.jpg) |
| `siamese-cat.jpg` | [Siamese cat HD](https://commons.wikimedia.org/wiki/File:Siamese_cat_HD.jpg) |
| `strawberries.jpg` | [Strawberries (249191619)](https://commons.wikimedia.org/wiki/File:Strawberries_(249191619).jpeg) |
| `tabby-cat.jpg` | [Tabby cat laying on the ground](https://commons.wikimedia.org/wiki/File:Tabby_cat_laying_on_the_ground.jpg) |
| `zebra.jpg` | [Zebra no Zoológico de Brasília março de 2025](https://commons.wikimedia.org/wiki/File:Zebra_no_Zool%C3%B3gico_de_Bras%C3%ADlia_mar%C3%A7o_de_2025.jpg) |
