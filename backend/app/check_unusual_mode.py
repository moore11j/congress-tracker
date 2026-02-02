from __future__ import annotations

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.routers import signals


def main() -> None:
    mode, applied_preset, preset_input = signals._resolve_unusual_mode(
        preset_input=None,
        recent_days=None,
        baseline_days=None,
        min_baseline_count=None,
        multiple=None,
        min_amount=None,
    )
    assert mode == "preset"
    assert applied_preset == signals.PRESET_DEFAULT
    assert preset_input is None

    mode, applied_preset, preset_input = signals._resolve_unusual_mode(
        preset_input="strict",
        recent_days=None,
        baseline_days=None,
        min_baseline_count=None,
        multiple=None,
        min_amount=None,
    )
    assert mode == "preset"
    assert applied_preset == "strict"
    assert preset_input == "strict"

    mode, applied_preset, preset_input = signals._resolve_unusual_mode(
        preset_input=None,
        recent_days=None,
        baseline_days=None,
        min_baseline_count=None,
        multiple=1.2,
        min_amount=None,
    )
    assert mode == "custom"
    assert applied_preset == "custom"
    assert preset_input is None

    mode, applied_preset, preset_input = signals._resolve_unusual_mode(
        preset_input="strict",
        recent_days=None,
        baseline_days=None,
        min_baseline_count=None,
        multiple=1.2,
        min_amount=None,
    )
    assert mode == "custom"
    assert applied_preset == "custom"
    assert preset_input == "strict"

    print("ok")


if __name__ == "__main__":
    main()
