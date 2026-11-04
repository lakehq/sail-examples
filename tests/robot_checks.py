"""Adversarial inputs for the robot examples, executed inside each engine environment."""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def example(name):
    path = ROOT / "multimodal" / name / "main.py"
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check(spark, engine):
    curate = example("curate-robot-episodes")
    camera = example("align-robot-camera-frames")
    schema = (
        "episode_index long, frame_index long, timestamp double, "
        "`observation.state` array<float>, action array<float>, task_index long"
    )
    rows = []
    for episode in range(7):
        for frame in range(3):
            rows.append((episode, frame, frame / 10, [1.0, 2.0], [3.0, 4.0], 0))
    rows = [r for r in rows if not (r[0] == 1 and r[1] == 1) and r[0] != 6]
    rows.append((2, 1, 0.1, [1.0, 2.0], [3.0, 4.0], 0))
    rows = [
        (
            ep,
            frame,
            0.9 if ep == 5 and frame == 1 else t,
            [float("nan"), 2.0] if ep == 3 and frame == 1 else state,
            [] if ep == 4 and frame == 1 else action,
            task,
        )
        for ep, frame, t, state, action, task in rows
    ]
    rows += [
        (7, 0, 0.0, [1.0, 2.0], [3.0, 4.0], 0),  # unlisted episode
        (8, 0, 0.0, [1.0, 2.0], [3.0, 4.0], None),
        (9, 0, None, [1.0, 2.0], [3.0, 4.0], 0),
        (10, 0, 0.0, [float("inf"), 2.0], [3.0, 4.0], 0),
        (11, 0, 0.0, [1.0, 2.0], [3.0, 4.0], 0),
        (11, 1, 0.1, [1.0, 2.0], [3.0, 4.0], None),
    ]
    episodes = spark.createDataFrame(
        [(i, 3) for i in range(7)] + [(i, 1) for i in (8, 9, 10)] + [(11, 2)],
        "episode_index long, length long",
    )
    tasks = spark.createDataFrame([(0, "push")], "task_index long, task string")
    results = curate.curate(spark.createDataFrame(rows, schema), episodes, tasks, 10)
    report = {r.episode_index: r.asDict() for r in results["quality"].collect()}
    assert {ep for ep, row in report.items() if row["usable"]} == {0}, report
    assert report[1]["frames"] == 2 and report[2]["duplicate_frames"] == 1
    assert report[3]["invalid_vectors"] == report[4]["invalid_vectors"] == 1
    assert report[5]["bad_timestamps"] > 0
    assert report[6]["frames"] == 0 and report[7]["expected_frames"] is None
    assert [r.episode_index for r in results["selection"].collect()] == [0]

    states = spark.createDataFrame(
        [
            (0, 0, 0.0, [1.0, 2.0], [3.0, 4.0], 0),
            (0, 1, 0.1, [5.0, 6.0], [7.0, 8.0], 0),
        ],
        schema,
    )
    samples = spark.createDataFrame(
        [
            (0, 0, 0.05, 0.05, 96, 96, "tie"),
            (0, 1, 0.151, 0.151, 96, 96, "outside"),
            (1, 0, 0.0, 0.0, 96, 96, "other-episode"),
        ],
        camera.SAMPLE_COLUMNS,
    )
    aligned = camera.align(states, samples, 10)
    output = [r.asDict() for r in aligned["alignment"].collect()]
    assert len(output) == 3 and output[0]["frame_index"] == 0, output
    assert not output[1]["matched"] and output[1]["state_x"] is None
    assert not output[2]["matched"] and output[2]["state_x"] is None
    coverage = {r.episode_index: r.asDict() for r in aligned["coverage"].collect()}
    assert coverage[0]["matched_frames"] == coverage[0]["unmatched_frames"] == 1
    assert coverage[1]["unmatched_frames"] == 1 and coverage[1]["max_time_error_seconds"] is None
    # The humanoid example: 28-joint vectors at 30 Hz, and every decoded frame kept even when unmatched.
    humanoid = example("curate-humanoid-episodes")
    joints, fps = [0.5] * 28, 30
    records = []
    for episode in range(5):
        for frame in range(4):
            records.append([episode, frame, frame / fps, list(joints), list(joints), 0])
    records = [r for r in records if (r[0], r[1]) != (1, 2)]  # episode 1 loses a record
    for r in records:
        if (r[0], r[1]) == (2, 3):
            r[2] += 0.01  # episode 2: one timestamp off the 30 Hz cadence
        if (r[0], r[1]) == (3, 1):
            r[3] = [float("nan")] + joints[1:]  # episode 3: a NaN joint
        if (r[0], r[1]) == (4, 0):
            r[4] = joints[:27]  # episode 4: an action vector one joint short
    humanoid_frames = spark.createDataFrame([tuple(r) for r in records], schema)
    humanoid_episodes = spark.createDataFrame([(i, 4) for i in range(5)], "episode_index long, length long")
    curated = humanoid.curate(humanoid_frames, humanoid_episodes, tasks, fps)
    quality = {r.episode_index: r.asDict() for r in curated["quality"].collect()}
    assert {ep for ep, row in quality.items() if row["usable"]} == {0}, quality
    assert quality[1]["frames"] == 3 and quality[2]["bad_timestamps"] > 0
    assert quality[3]["invalid_vectors"] == quality[4]["invalid_vectors"] == 1
    assert [r.episode_index for r in curated["selection"].collect()] == [0]

    decoded = spark.createDataFrame(
        [
            ("cam", 0, 0, 0.0, 0.0, 640, 480, "a"),
            ("cam", 0, 1, 1 / 60, 1 / 60, 640, 480, "tie"),
            ("cam", 0, 2, 0.2, 0.2, 640, 480, "outside"),
        ],
        humanoid.FRAME_COLUMNS,
    )
    matched = humanoid.align(humanoid_frames.filter("episode_index = 0"), decoded, fps)
    rows = [r.asDict() for r in matched["alignment"].collect()]
    assert [r["frame_index"] for r in rows] == [0, 0, None], rows
    assert [r["matched"] for r in rows] == [True, True, False]
    coverage = matched["coverage"].collect()[0].asDict()
    assert (
        coverage["decoded_frames"] == 3
        and coverage["matched_frames"] == 2
        and coverage["distinct_records"] == 1
    )
    return {
        "engine": engine,
        "quality_episodes": len(report),
        "alignment_samples": len(output),
        "humanoid_episodes": len(quality),
        "humanoid_frames": len(rows),
    }
