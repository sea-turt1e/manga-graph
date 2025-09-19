from typing import Any, Dict

from infrastructure.external.neo4j_repository import Neo4jMangaRepository


def test_same_publisher_other_magazines_uses_period_filter(monkeypatch):
    repo = Neo4jMangaRepository(driver=None)

    # main works: anchor with years 2012-2017, publisher 集英社, magazine 週刊少年ジャンプ
    main_works = [
        {
            "work_id": "anchor_work_1",
            "title": "テスト作品A",
            "first_published": "2012-01-01",
            "last_published": "2017-12-31",
            "total_volumes": 24,
            "creators": ["作者A"],
            "magazines": ["週刊少年ジャンプ"],
            "publishers": ["集英社"],
            "genre": "少年",
        }
    ]

    # Patch search_manga_works / publications to avoid hitting DB
    monkeypatch.setattr(repo, "search_manga_works", lambda term, limit: main_works)
    monkeypatch.setattr(repo, "search_manga_publications", lambda term, limit: [])

    captured: Dict[str, Any] = {}

    def fake_run(query: str, **params):
        # Detect same-publisher-other-magazines query
        if "UNWIND $publisher_names AS pubName" in query:
            # Query should contain our period filter and scoring
            assert "WHERE w_start IS NOT NULL AND w_end IS NOT NULL" in query
            assert "w_end >= af - yw AND w_start <= al + yw" in query
            assert "overlap_years AS overlap_years" in query
            assert "gap AS period_gap" in query
            assert "jaccard_similarity" in query
            # Params should include anchor years/window and anchor authors
            assert params.get("anchor_first_year") == 2012
            assert params.get("anchor_last_year") == 2017
            assert params.get("year_window") == 2
            assert isinstance(params.get("anchor_authors_lower"), list)
            assert params.get("limit") == 5
            captured["checked"] = True
            # Return one dummy result
            return [
                {
                    "work_id": "other_work_1",
                    "title": "別誌の同時代作品",
                    "first_published": "2013-01-01",
                    "last_published": "2016-01-01",
                    "total_volumes": 10,
                    "creators": ["作者B"],
                    "magazine_name": "週刊ヤングジャンプ",
                    "publisher_name": "集英社",
                    "overlap_years": 4,
                    "period_gap": 0,
                    "jaccard_similarity": 0.5,
                }
            ]
        # Detect magazine->publisher mapping query
        if "UNWIND $work_ids AS wid" in query and "PUBLISHED_BY" in query:
            return []
        # Any other calls return empty
        return []

    monkeypatch.setattr(repo, "_run", fake_run)

    result = repo.search_manga_data_with_related(
        search_term="テスト",
        limit=10,
        include_related=False,
        include_same_publisher_other_magazines=True,
        same_publisher_other_magazines_limit=5,
    )

    # Ensure our query path was exercised
    assert captured.get("checked") is True

    work_nodes = [n for n in result["nodes"] if n.get("type") == "work"]
    work_ids = {n["id"] for n in work_nodes}
    # Should contain the dummy other work
    assert "other_work_1" in work_ids
    # And the jaccard similarity is present in properties of the work node
    target = next(n for n in work_nodes if n["id"] == "other_work_1")
    assert target.get("properties", {}).get("jaccard_similarity") is not None
