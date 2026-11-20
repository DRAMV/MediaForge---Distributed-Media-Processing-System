import json
import math
import pathlib
import shutil
import subprocess
import tempfile
import threading

from common import config

_classifier = None 
_classifier_lock = threading.Lock()

def _get_classifier():
    global _classifier 

    if _classifier is None:
        from transformers import pipeline

        _classifier = pipeline(
            task="image-classification",
            model=config.MODERATION_MODEL,
            device=-1,
            model_kwargs={"use_safetensors": True},
        )

    return _classifier 


def warmup():
    with _classifier_lock:
        _get_classifier()


def run(job):
    from PIL import Image

    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")

    if not ffmpeg or not ffprobe:
        raise RuntimeError("Moderation requires FFmpeg and FFprobe")

    interval = config.MODERATION_INTERVAL
    threshold = config.MODERATION_THRESHOLD
    maximum = config.MODERATION_MAX_SECONDS

    if not math.isfinite(interval) or interval <= 0:
        raise ValueError("MODERATION_INTERVAL must be positive and finite")

    if not math.isfinite(threshold) or not 0 <= threshold <= 1:
        raise ValueError("MODERATION_THRESHOLD must be between 0 and 1")

    if not math.isfinite(maximum) or maximum <=0:
        raise ValueError("MODERATION_MAX_SECONDS must be positive and finite")

    with tempfile.TemporaryDirectory(prefix="mediaforge-moderation-") as temp:
        directory = pathlib.Path(temp)
        suffix = pathlib.Path(job.filename).suffix or ".mp4"

        source = directory / f"input{suffix}"
        source.write_bytes(job.data)

        probe = subprocess.run(
            [ffprobe,
             "-v","error",
             "-show_entries", "format=duration",
             "-of", "json",
             str(source),
             ],
             capture_output=True,
             text=True,
             timeout=15,
        )

        if probe.returncode != 0:
            raise RuntimeError(
                f"FFprobe failed: {probe.stderr[-1000:]}"
            )

        metadata = json.loads(probe.stdout)

        duration = float(
            metadata.get("format", {}).get("duration", "nan")
        )

        if not math.isfinite(duration) or duration <= 0:
            raise ValueError("Cannot determine a valid video duration")

        if duration > maximum:
            raise ValueError(
                f"Moderation demo accepts videos up to {maximum:g} seconds"
            )

        output_pattern = directory / "frame_%06d.jpg"

        extraction = subprocess.run(
            [
                ffmpeg,
                "-nostdin",
                "-hide_banner",
                "-loglevel", "error",
                "-y",
                "-i", str(source),
                "-map", "0:v:0",
                "-an",
                "-vf",
                f"fps=1/{interval}:start_time=0,scale=320:-1",
                "-q:v", "3",
                str(output_pattern),
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )

        if extraction.returncode != 0:
            raise RuntimeError(
                f"Frame extraction failed: {extraction.stderr[-1000:]}"
            )

        frames = sorted(directory.glob("frame_*.jpg"))

        if not frames:
            raise ValueError("No Video frames could be extracted")

        results = []

        with _classifier_lock:
            classifier = _get_classifier()

            for number, frame in enumerate(frames, start=1):
                with Image.open(frame) as image:
                    rgb = image.convert("RGB")

                    try:
                        predictions = classifier(rgb, top_k=2)

                    finally:
                        rgb.close()

                scores = {
                    item["label"].lower(): float(item["score"])
                    for item in predictions
                }

                if "nsfw" not in scores:
                    raise RuntimeError(
                        "Classifier did not return the expected 'nsfw' label"
                    )

                score = scores['nsfw']

                results.append({
                    "sampled_frame_number": number,
                    "nsfw_score": round(score, 6),
                    "review_required": score >= threshold,
                })

    flagged = sum(item["review_required"] for item in results)

    status = (
        "REVIEW_REQUIRED"
        if flagged 
        else "NO_FLAGS_IN_SAMPLED_FRAMES"
    )

    report = {
        "job_id": job.job_id,
        "filename": job.filename,
        "task": "moderation",
        "scope": "sampled visual explicit-content screening",
        "model": config.MODERATION_MODEL,
        "status": status,
        "duration_seconds": duration,
        "sample_interval_seconds": interval,
        "threshold": threshold,
        "frames_checked": len(results),
        "frames_flagged": flagged,
        "frames": results,
        "limitations": [
            "Frames between samples were not assessed",
            "Audio and speech were not assessed",
            "This model is not a general violence or hate-speech detector",
            "Model scores and flags can be incorrect",
        ],
    }

    data = json.dumps(report, indent=2, ensure_ascii=False).encode("utf-8")

    detail = (
        f"moderation: {status};"
        f"checked={len(results)}; flagged={flagged}"
    )

    return data, detail