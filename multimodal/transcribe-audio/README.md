---
title: Transcribe audio files with Whisper
id: transcribe-audio
facts: [load-files-through-a-path-table, udf-setup-per-partition]
summary: Turn audio files into transcripts and check their accuracy.
description: Use when you need speech-to-text for a folder of audio files inside the engine (Whisper on CPU with faster-whisper), one transcript per file, and want to score the transcripts against references in SQL.
tier: multimodal
level: intermediate
tags: [audio, speech, transcription, whisper, faster-whisper, mapInArrow, evaluation]
modality: audio
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 240
needs_key: false
needs_gpu: false
prompt: Transcribe a folder of English speech clips with Whisper tiny.en (faster-whisper on CPU) on Sail, one transcript per file, and compute the character error rate against reference transcripts in SQL.
---

# Transcribe audio files with Whisper

This example transcribes 73 short clips of read English speech (about 8 minutes in all) with Whisper tiny.en, running on the CPU inside the engine. It then compares each transcript with the clip's reference transcript in SQL and reports the character error rate.

## Run it

Run all commands below from the repository root (`sail-examples/`):

```bash
uv run multimodal/transcribe-audio/main.py
```

The first run downloads the sample clips (a 9 MB Parquet file) and the Whisper tiny.en model converted for CTranslate2 (about 80 MB) from Hugging Face into the local Hugging Face cache. No account or key is needed. The dataset and model revisions are pinned.

On these 73 sample clips, the transcripts have a character error rate of about 5% against the references. Some differences are spelling choices rather than mistakes: Whisper writes "Mr." where the reference has "MISTER".

<!-- sail-output:start variant=default -->

## Example output

Recorded output · Sail 0.7.3.

The example has changed since this capture. This output comes from the earlier script.

```text
+----------------+-------+--------------------------------------------------------------------------------+
|              id|seconds|                                                                      transcript|
+----------------+-------+--------------------------------------------------------------------------------+
|1272-128104-0000|  5.855|Mr. Quilter is the apostle of the middle classes and we are glad to welcome h...|
|1272-128104-0001|  4.815|                   Nor is Mr. Quilter's manner less interesting than his matter.|
|1272-128104-0002| 12.485|He tells us that at this festive season of the year, with Christmas and roast...|
|1272-128104-0003|    9.9|He has grave doubts whether Sir Frederick Layton's work is really Greek after...|
|1272-128104-0004|   29.4|Linnell's pictures are a sort of up-guards and atom paintings, and Mason's ex...|
+----------------+-------+--------------------------------------------------------------------------------+
only showing top 5 rows

+-----+-------+--------------------+
|clips|minutes|character_error_rate|
+-----+-------+--------------------+
|   73|   8.02|0.054469721243191285|
+-----+-------+--------------------+

```

<!-- sail-output:end -->

## How it works

- **A table of paths.** The clips are saved as FLAC files, and a DataFrame lists one `path` per file with its reference transcript. Each task opens only the files in its own rows.
- **Load once per partition.** `mapInArrow` downloads the model where the task runs (cached per machine), then loads it with [faster-whisper](https://github.com/SYSTRAN/faster-whisper) before its loop over batches. Each model uses one CPU thread, because the engine already runs partitions in parallel.
- **Transcribe.** faster-whisper decodes each file to 16 kHz mono audio and transcribes it. Decoding is greedy at a single temperature of 0, with no sampled retries. Runtime and numerical differences can still affect transcripts.
- **Save once.** The transcripts are written to Parquet and read back, so the scoring query never runs the model again.
- **Score in SQL.** Both texts are lowercased and reduced to ASCII letters, digits, apostrophes and single spaces with explicit character classes, `levenshtein` counts the character edits between them, and the error rate is the total edits divided by the total reference characters.

## Evaluate the full test set

`multimodal/transcribe-audio/full_test_clean.py` runs the same transcription function on all of LibriSpeech test-clean: 2,620 utterances, 5.4 hours of read speech. It normalizes both texts with Whisper's English text normalizer (from `transformers`, with its spelling list from the pinned `openai/whisper-tiny.en` repository), as the [Whisper paper](https://arxiv.org/abs/2212.04356) (Radford et al., 2022) did for its word error rates. It then computes the corpus word error rate in SQL: word edits per utterance from a `pandas_udf`, summed over all utterances and divided by the reference word count.

```bash
uv run multimodal/transcribe-audio/full_test_clean.py
```

The first run downloads the test set (a 350 MB Parquet file) and the normalizer's spelling list. The word error rate comes out at 5.9%. The Whisper paper reports 5.6% for tiny.en on test-clean with greedy decoding (Appendix D.1.1, Table 8). This run uses faster-whisper's CTranslate2 version of the model, not OpenAI's own code.

## Use your own audio

Replace `download_clips` with a list of your own files. faster-whisper decodes audio with PyAV, which bundles the FFmpeg libraries (per the faster-whisper README), so it reads the formats that bundled FFmpeg supports. The paths must be readable from wherever the UDF runs: local paths on one machine, object storage on a cluster (open the file with a client such as `fsspec` and pass the file object to `transcribe`). Without reference transcripts, drop the scoring step. A file that cannot be decoded raises an error and fails the job. To record such files and keep going, catch the error per file and return a status column, as the [OCR example](../ocr-scanned-pdfs/README.md) does.

For other languages, use a multilingual model such as `Systran/faster-whisper-small` and set `language`. Whisper's own checkpoints whose names end in `.en` are English-only; the others are multilingual (per the Whisper model card).

## Data

The clips are from the [LibriSpeech ASR corpus](https://www.openslr.org/12) (Vassil Panayotov, Guoguo Chen, Daniel Povey and Sanjeev Khudanpur, ICASSP 2015), licensed under CC BY 4.0, by way of the [`hf-internal-testing/librispeech_asr_dummy`](https://huggingface.co/datasets/hf-internal-testing/librispeech_asr_dummy) sample on Hugging Face. The full evaluation reads the test-clean split of the same corpus from the [`openslr/librispeech_asr`](https://huggingface.co/datasets/openslr/librispeech_asr) dataset on Hugging Face, at a pinned revision.
