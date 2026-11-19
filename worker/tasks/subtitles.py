import io
import threading

from common import config


_model = None 
_model_lock = threading.Lock()


def _get_model():
    global _model 

    if _model is None:
        from faster_whisper import WhisperModel

        _model = WhisperModel(
            config.SUBTITLE_MODEL,
            device="cpu",
            compute_type="int8",
            cpu_threads=2,
        )

    return _model

def warmup():
    
    with _model_lock:
        _get_model()

def _srt_time(milliseconds):

    hours, reminder = divmod(milliseconds, 3_600_000)
    minutes, remainder = divmod(reminder, 60_000)
    seconds, milliseconds = divmod(remainder, 1_000)

    return (
        f"{hours:02d}:{minutes:02d}:{seconds:02d},"
        f"{milliseconds:03d}"
    )

def run(job):
    with io.BytesIO(job.data) as media:
        with _model_lock:
            model = _get_model()
            segments, info = model.transcribe(
                media ,
                language=config.SUBTITLE_LANGUAGE or None,
                task="transcribe",
                beam_size=5,
                vad_filter=True,
                condition_on_previous_text=False,
            )

            segments = list(segments)

    blocks = []

    for segment in segments:
        text = " ".join(segment.text.split())

        if not text:
            continue

        start_ms = max(0, round(segment.start * 1_000))
        end_ms = max(start_ms + 1, round(segment.end * 1_000))

        number = len(blocks) + 1

        blocks.append(
            f"{number}\n"
            f"{_srt_time(start_ms)} --> {_srt_time(end_ms)}\n"
            f"{text}\n\n"
        )

    if not blocks:
        raise ValueError(
            "No speech was recognized; no subtitle file was generated"
        )

    result = "".join(blocks).encode("utf-8")

    detail = (
        f"subtitles: created SRT; "
        f"language={info.language}; cues={len(blocks)}"
    )

    return result, detail