import pathlib
import shutil
import subprocess
import tempfile
import time

from common import config


def run(job):
    ffmpeg = shutil.which("ffmpeg")

    if not ffmpeg:
        time.sleep(config.PROCESS_SECONDS)
        #fallback path
        return (
            job.data,
            "transcode: simulated conversion; "
            "FFmpeg unavailable; original file returned",
        )

    with tempfile.TemporaryDirectory(prefix="mediaforge-") as directory:
        directory = pathlib.Path(directory)
        suffix = pathlib.Path(job.filename).suffix or ".mp4"

        source = directory / f"input{suffix}"
        output = directory / "output.mp4"

        source.write_bytes(job.data)
        result = subprocess.run(
            [
                ffmpeg,
                "-nostdin",
                "-hide_banner",
                "-loglevel","error",
                "-y",
                "-i", str(source),
                "-map", "0:v:0",
                "-map", "0:a?",
                "-c:v", "libx264",
                "-preset", "ultrafast",
                "-pix_fmt", "yuv420p",
                "-c:a", "aac",
                "-movflags", "+faststart",
                str(output)
            ],
            capture_output=True,
            timeout=20,
        )

        if result.returncode != 0:
            message = result.stderr.decode(errors="replace")
            raise RuntimeError(f"FFmpeg failed: {message[-1000:]}")

        return (
            output.read_bytes(),
            "transcode: converted video to H.264/AAC MP4",
        )

                           





