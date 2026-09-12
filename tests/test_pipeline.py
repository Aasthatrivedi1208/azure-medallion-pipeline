import json
from pathlib import Path


FIXTURE = Path(__file__).parents[1] / "fixtures" / "synthetic_chapters.json"


def invalid_coordinates(feature: dict) -> bool:
    geometry = feature.get("geometry") or {}
    try:
        longitude = float(geometry.get("x"))
        latitude = float(geometry.get("y"))
    except (TypeError, ValueError):
        return True
    return not (-180 <= longitude <= 180 and -90 <= latitude <= 90)


def test_fixture_covers_warning_and_quarantine_paths():
    features = json.loads(FIXTURE.read_text())["features"]
    invalid_ids = {
        feature["attributes"]["ChapterID"]
        for feature in features
        if invalid_coordinates(feature)
    }
    warning_ids = {
        feature["attributes"]["ChapterID"]
        for feature in features
        if not invalid_coordinates(feature)
        and (
            not str(feature["attributes"].get("City") or "").strip()
            or str(feature["attributes"].get("City")).strip().upper() == "UNKNOWN"
        )
    }

    assert invalid_ids == {"CA-9101", "CA-9102", "CA-9103"}
    assert warning_ids == {"CA-9002", "CA-9003"}
