import pytest
from celery import group, chain
from paper_collector import pipelines
from paper_collector.tasks import (
    scan_openalex,
    enrich_crossref,
    classify_oa,
    fetch_pdf_if_oa,
    summarize_from_bundle,
    save_to_db,
    index_search,
)

# --- Test Data ---
SAMPLE_DOIS = ["10.1234/1", "10.5678/2"]

# --- Integration Tests for Pipelines ---

@pytest.fixture
def mock_celery_canvas(mocker):
    """Mocks celery chain, group, and task signatures."""
    mocker.patch("celery.chain.apply_async")
    mocker.patch("celery.group.apply_async")
    mocker.patch("paper_collector.pipelines.scan_openalex.s", return_value=scan_openalex.s())
    mocker.patch("paper_collector.pipelines.process_dois.s")
    return {
        "chain": mocker.spy(pipelines, "chain"),
        "group": mocker.spy(pipelines, "group"),
    }

def test_start_harvest_pipeline(mocker):
    """Test that start_harvest creates the correct pipeline chain."""
    mock_scan = mocker.patch("paper_collector.tasks.scan_openalex.s")
    mock_process = mocker.patch("paper_collector.pipelines.process_dois.s")
    mock_pipeline = mocker.MagicMock()
    # Mock the chain operator `|`
    mock_scan.return_value.__or__.return_value = mock_pipeline

    pipelines.start_harvest.s().apply()

    mock_scan.assert_called_once()
    mock_process.assert_called_once()
    mock_pipeline.apply_async.assert_called_once()

def test_process_dois_pipeline(mocker):
    """Test that process_dois creates a group of processing chains."""
    mock_group = mocker.patch("celery.group")
    mock_chain = mocker.patch("celery.chain")

    # Call the task directly to test its logic
    pipelines.process_dois(SAMPLE_DOIS)

    # Assert that a group was created
    mock_group.assert_called_once()

    # Assert that a chain was created for each DOI
    assert mock_chain.call_count == len(SAMPLE_DOIS)

    # Check the structure of one of the chains to ensure correctness
    first_chain_args = mock_chain.call_args_list[0].args
    task_signatures = [
        enrich_crossref.s(SAMPLE_DOIS[0]),
        classify_oa.s(),
        fetch_pdf_if_oa.s(),
        summarize_from_bundle.s(),
        save_to_db.s(),
        index_search.s()
    ]

    # Compare the signatures inside the chain call
    called_signatures = first_chain_args[0]
    for i, sig in enumerate(task_signatures):
        assert called_signatures[i].task == sig.task
        assert called_signatures[i].args == sig.args

def test_backfill_batch_pipeline(mocker):
    """Test that backfill_batch creates the correct pipeline."""
    mock_scan_backfill = mocker.patch("paper_collector.tasks.scan_openalex_by_date_range.s")
    mock_process = mocker.patch("paper_collector.pipelines.process_dois.s")
    mock_pipeline = mocker.MagicMock()
    mock_scan_backfill.return_value.__or__.return_value = mock_pipeline

    year = 2023
    pipelines.backfill_batch(year)

    mock_scan_backfill.assert_called_once_with(
        from_date=f"{year}-01-01",
        to_date=f"{year}-12-31",
        max_results=5000
    )
    mock_process.assert_called_once()
    mock_pipeline.apply_async.assert_called_once()

def test_process_dois_no_dois():
    """Test that process_dois handles an empty list of DOIs gracefully."""
    # This is more of a sanity check, doesn't need mocks
    result = pipelines.process_dois([])
    assert result is None # The task should do nothing and exit