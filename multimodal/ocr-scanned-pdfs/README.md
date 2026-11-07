---
title: Extract text from scanned PDFs with OCR
id: ocr-scanned-pdfs
description: Use when you need the text of scanned or image-only PDFs (no text layer) inside the engine, one row per page with an explicit status for blank pages and unreadable files, using OCR on the CPU (RapidOCR with ONNX Runtime), and want to measure OCR accuracy in SQL.
tier: multimodal
level: advanced
tags: [pdf, ocr, documents, scans, rapidocr, onnx, pypdfium2, mapInArrow, evaluation]
modality: document
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 180
needs_key: false
needs_gpu: false
prompt: Read a folder of scanned PDFs on Sail, render each page with pypdfium2, extract its text with RapidOCR on the CPU, return one row per page with a status of ok, blank or error (with the reason) and the mean OCR confidence, and score the text against transcriptions in SQL with a character error rate (as read and with the words sorted) and a word recall.
---

# Extract text from scanned PDFs with OCR

A scanned PDF holds pictures of pages, not text, so a PDF text extractor finds nothing in it. This example reads real scans of printed pages from 1888 to 1915 inside the engine: a book page, a newspaper column from microfilm, a fare table, and a handbill downscaled to low resolution. It puts them in image-only PDFs, renders every page, reads it with OCR, and scores the text against a transcription of each page. One PDF also has a blank page, and one file is not a PDF at all, to show how failures are reported.

## Run it

```bash
uv run main.py
```

The OCR models ship inside the `rapidocr` package, so nothing is downloaded at run time.

| Page | Character error rate | Same, words sorted | Word recall |
|---|---|---|---|
| Book page, 1896 | 0.0 | 0.0 | 1.0 |
| Newspaper column, 1915 | 0.032 | 0.061 | 0.95 |
| Handbill, 1888, downscaled | 0.043 | 0.049 | 0.932 |
| Fare table, 1892 | 0.314 | 0.02 | 0.981 |

The fare table shows why the example reports more than one measure. OCR returns the table one printed line at a time across its columns, so a cell printed on two lines comes out in two pieces, while the transcription keeps each cell whole. The printout shows the first lines of both. The character error rate counts order, so it is 0.314. With the words of both texts sorted it is 0.02, and the word recall is 0.981, so almost all of the 0.314 comes from the order, not from misread words. The blank page comes back as `blank`, and the file that is not a PDF as one `error` row whose `error` column gives the reason.

<!-- sail-output:start variant=default -->

## Example output

Recorded output · Sail 0.7.3.

The example has changed since this capture. This output comes from the earlier script.

```text
+--------------------------------+----+------+-----+----------+-----+----------------+-----------+-----------------------------------------------------------------+
|file                            |page|status|lines|confidence|cer  |cer_sorted_words|word_recall|error                                                            |
+--------------------------------+----+------+-----+----------+-----+----------------+-----------+-----------------------------------------------------------------+
|damaged.pdf                     |NULL|error |0    |NULL      |NULL |NULL            |NULL       |PdfiumError: Failed to load document (PDFium: Data format error).|
|fifty-famous-stories-1896.pdf   |1   |ok    |11   |0.9878    |0.0  |0.0             |1.0        |NULL                                                             |
|fifty-famous-stories-1896.pdf   |2   |blank |0    |NULL      |0.0  |0.0             |NULL       |NULL                                                             |
|hack-fares-1892.pdf             |1   |ok    |39   |0.9929    |0.314|0.02            |0.981      |NULL                                                             |
|street-railway-handbill-1888.pdf|1   |ok    |27   |0.9778    |0.043|0.049           |0.932      |NULL                                                             |
|university-missourian-1915.pdf  |1   |ok    |20   |0.9775    |0.032|0.061           |0.95       |NULL                                                             |
+--------------------------------+----+------+-----+----------+-----+----------------+-----------+-----------------------------------------------------------------+

Highest character error rate, hack-fares-1892.pdf page 1:
  OCR                                                  TRANSCRIPTION
  DISTRICT OF COLUMBIA,                                DISTRICT OF COLUMBIA,
  RATES OF FARE FOR PUBLIC CONVEYANCE OF PASSENGERS.   RATES OF FARE FOR PUBLIC CONVEYANCE OF PASSENGERS.
  ONE-HORSE VEHICLES.                                  ONE-HORSE VEHICLES.
  MidnightRates.                                       Day Rates.
  Day                                                  Between 5 A. M. and 12.30 A. M.
  Rates.                                               Midnight Rates.
  Between 5 A. M. and 12.30 A. M.                      Between 12.30 A. M. and 5 A. M.
  Between 12.30 A. M. and 5 A. M.                      BY THE TRIP.
  BY THE TRIP.                                         Fifteen Squares or Less.
  Fifteen Squares                                      Each Additional Five Squares or parts of Squares.
  Each Additional Five Squares                         Fifteen Squares. or Less.
  Fifteen Squares.                                     Each Additional Five Squares or parts of Squares.
  Each Additional Five Squares                         Each Passenger
  or Less.                                             25 Cts.
  or parts of Squares.                                 10 Cents.
  or Less.                                             40 Cts.

+------+-----+-------------------------+----------------+
|status|pages|mean_character_error_rate|mean_word_recall|
+------+-----+-------------------------+----------------+
| blank|    1|                      0.0|            NULL|
| error|    1|                     NULL|            NULL|
|    ok|    4|                   0.0972|          0.9657|
+------+-----+-------------------------+----------------+

```

<!-- sail-output:end -->

## How it works

- **A table of paths.** The PDFs are listed in a DataFrame with one `path` per file. Each task opens only the files in its own rows.
- **One PDF in, one row per page out.** `mapInArrow` creates the OCR engine once per partition. For each file, [pypdfium2](https://github.com/pypdfium2-team/pypdfium2) renders every page at two pixels per PDF point, one page at a time under a lock (see On Sail). Each scan is placed in its PDF at 144 dpi, so the rendered page has the same size in pixels as the scan. [RapidOCR](https://github.com/RapidAI/RapidOCR) then finds the lines of text and reads them. Each page is emitted in its own output batch before the next page is processed. Memory still depends on the size of an individual rendered page and the OCR models.
- **An explicit status per page.** A page with text is `ok`, and a page where OCR keeps no line is `blank`. A file that cannot be opened (missing, unreadable or not a PDF) is a single `error` row. A page that fails to render or read is an `error` row for that page. Each `error` row carries the exception in its `error` column. One bad file never stops the job, and nothing disappears silently.
- **A confidence per page.** The mean of RapidOCR's scores for the lines it kept. A low value marks a page worth checking by hand. RapidOCR drops any line it reads with a score below 0.5 (its default, set explicitly as `MIN_LINE_SCORE`), so `blank` means no line scored 0.5 or more.
- **Save once.** The pages are written to Parquet and read back, so later queries never run OCR again.
- **Score in SQL.** Both texts are lowercased, with typographic quotes and dashes made plain and whitespace collapsed, using explicit character classes.
  - The **character error rate** is `levenshtein` between the two texts, divided by the length of the transcription.
  - The same rate **with the words sorted** compares the two texts after sorting the words of each (`array_sort`), so reading order no longer counts. Sorting can also move a misread word away from the word it stands for, so on a page read in order it can be higher than the plain rate.
  - The **word recall** is the share of the transcription's distinct words found anywhere in the OCR text: `size(array_intersect(...)) / size(...)`. It is null for a page with no words.

## Use your own PDFs

Replace `make_pdfs` with a list of your own files, and drop the scoring step if you have no transcriptions. The paths must be readable from wherever the UDF runs: local paths on one machine, object storage on a cluster. pypdfium2 also opens bytes, so you can read the file with a client such as `fsspec` and pass its contents.

For PDFs that do have a text layer, read it directly (see the [PDF search example](../pdf-search/README.md)): that needs no OCR and extracts the embedded text. Run OCR on the pages where it comes back empty.

Per RapidOCR's model list (`default_models.yaml` in the package), the bundled model family covers Chinese, Japanese, English and many languages written in Latin script. See its documentation for others.

## On Sail

In local mode, Sail runs the partitions of a Python UDF as threads of one process. PDFium, the PDF library under pypdfium2, must never be called from two threads at once (per the pypdfium2 docs). So every PDFium call in this example (open, render, close) goes through one lock for the whole process, from `process_lock`.

- **Where the lock lives.** In `sys.modules`, because each task gets its own copy of the script's globals.
- **What the lock costs.** Rendering is serialized across the whole process, but OCR runs outside the lock, so partitions still read pages in parallel.
- **Other code.** Anything else in the same process that calls pypdfium2, such as another UDF, must take the same `process_lock("pdfium")`.

Use the same pattern for any library that is not safe to call from several threads at once.

## Scans

All four scans are of material published in the United States before 1929, from the Internet Archive and the Library of Congress. [scans/CREDITS.md](scans/CREDITS.md) lists each source and its rights statement.
