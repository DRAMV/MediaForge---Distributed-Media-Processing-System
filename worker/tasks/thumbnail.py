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
        return (
            job.data,
            "thumbnail: simulated extraction; "
            "FFmpeg unavailable; original file returned",
        )

    with tempfile.TemporaryDirectory(prefix="mediaforge-") as directory:
        directory = pathlib.Pth(directory)
        suffix = pathlib.Path(job.filename).suffix or ".mp4"

        source = directory / f"input{suffix}"
        output = directory / "thumbnail.jpg"

        source.write_bytes(job.data)
        result = subprocess.run (
            [
                ffmpeg,
                "-nostdin",
                "-hide_banner",
                "-loglevel", "error",
                "-y",
                "-i", str(source),
                "-franes:v", "1",
                "-vf", "scale=320:-1",
            ],
            capture_output=True,
            timeout=20,
            )

        if result.returncode != 0:
            message = result.stderr.decode(errors="replace")
            raise RuntimeError(f"FFmpeg failed: {message[-1000:]}")

        return (
            output.read_bytes(),
            "thumbnail: created JPEG preview from first frame",
        )

