from simulator.catalog import Track, artists, load_catalog


def test_catalog_loads_every_track():
    tracks = load_catalog()
    assert len(tracks) == 32
    assert all(isinstance(t, Track) for t in tracks)


def test_track_ids_are_unique_and_spotify_shaped():
    tracks = load_catalog()
    ids = [t.track_id for t in tracks]
    assert len(set(ids)) == len(ids)
    assert all(len(i) == 22 and i.isalnum() for i in ids)


def test_every_track_has_a_plausible_duration():
    for track in load_catalog():
        assert 60_000 < track.duration_ms < 900_000


def test_artists_are_unique_and_sorted():
    names = artists(load_catalog())
    assert names == sorted(set(names))
    assert len(names) == 8
