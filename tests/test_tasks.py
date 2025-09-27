import pytest
from unittest.mock import MagicMock, mock_open
from paper_collector import tasks

# --- Test Data ---
SAMPLE_DOI = "10.1234/sample.doi"
SAMPLE_CROSSREF_RESPONSE = {
    "message": {
        "title": ["Sample Title"],
        "author": [{"given": "J.", "family": "Doe"}],
        "abstract": "<jats:p>This is an abstract.</jats:p>"
    }
}
SAMPLE_UNPAYWALL_OA_RESPONSE = {
    "doi": SAMPLE_DOI,
    "is_oa": True,
    "oa_status": "gold",
    "license": "cc-by",
    "best_oa_location": {"url_for_pdf": "http://example.com/sample.pdf"}
}
SAMPLE_UNPAYWALL_NON_OA_RESPONSE = {
    "doi": SAMPLE_DOI,
    "is_oa": False
}
SAMPLE_OPENALEX_RESPONSE = {
    "results": [
        {"doi": f"https://doi.org/{SAMPLE_DOI}"},
        {"doi": "https://doi.org/10.5678/another.doi"},
    ]
}
SAMPLE_TOPICS_YAML = """
cs:
  include: ["large language model"]
  exclude: ["astronomy"]
"""

# --- Tests for Helper Functions ---

def test_load_topics(mocker):
    """Test that topics are loaded correctly from YAML."""
    mocker.patch("builtins.open", mock_open(read_data=SAMPLE_TOPICS_YAML))
    mocker.patch("yaml.safe_load", return_value={"cs": {"include": ["test"]}})

    topics = tasks.load_topics("dummy/path.yml")
    assert "cs" in topics
    assert topics["cs"]["include"] == ["test"]

def test_build_openalex_filter():
    """Test the OpenAlex filter builder logic."""
    topics = {"cs": {"include": ["llm"], "exclude": ["quantum"]}}
    filter_str = tasks.build_openalex_filter(topics, from_date="2023-01-01", to_date="2023-01-31")
    assert "display_name.search:llm" in filter_str
    assert "-display_name.search:quantum" in filter_str
    assert "from_publication_date:2023-01-01" in filter_str
    assert "to_publication_date:2023-01-31" in filter_str

# --- Tests for Celery Tasks ---

@pytest.fixture
def mock_requests_get(mocker):
    """Fixture to mock requests.get."""
    return mocker.patch("requests.get")

def test_scan_openalex(mock_requests_get):
    """Unit test for the scan_openalex task."""
    mock_response = MagicMock()
    mock_response.json.return_value = SAMPLE_OPENALEX_RESPONSE
    mock_response.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_response

    # Mock topics to avoid file I/O
    tasks.load_topics = lambda: {"cs": {"include": ["test"]}}

    result = tasks.scan_openalex.s().apply().get()

    assert len(result) == 2
    assert SAMPLE_DOI in result

def test_enrich_crossref(mock_requests_get):
    """Unit test for the enrich_crossref task."""
    mock_response = MagicMock()
    mock_response.json.return_value = SAMPLE_CROSSREF_RESPONSE
    mock_response.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_response

    bundle = tasks.enrich_crossref.s(SAMPLE_DOI).apply().get()

    assert bundle["doi"] == SAMPLE_DOI
    assert "crossref_data" in bundle
    assert bundle["crossref_data"]["title"] == ["Sample Title"]

def test_classify_oa(mock_requests_get):
    """Unit test for the classify_oa task."""
    mock_response = MagicMock()
    mock_response.json.return_value = SAMPLE_UNPAYWALL_OA_RESPONSE
    mock_response.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_response

    initial_bundle = {"doi": SAMPLE_DOI, "crossref_data": {}}
    result_bundle = tasks.classify_oa.s(initial_bundle).apply().get()

    assert "oa_data" in result_bundle
    assert result_bundle["oa_data"]["is_oa"] is True
    assert result_bundle["oa_data"]["license"] == "cc-by"

def test_fetch_pdf_if_oa():
    """Test the conditional PDF fetch logic."""
    # Case 1: Is OA
    oa_bundle = {
        "doi": SAMPLE_DOI,
        "oa_data": {"is_oa": True, "best_oa_location": {"url_for_pdf": "http://a.b/c.pdf"}}
    }
    result_oa = tasks.fetch_pdf_if_oa.s(oa_bundle).apply().get()
    assert "pdf_path" in result_oa
    assert result_oa["pdf_path"] is not None

    # Case 2: Not OA
    non_oa_bundle = {"doi": SAMPLE_DOI, "oa_data": {"is_oa": False}}
    result_non_oa = tasks.fetch_pdf_if_oa.s(non_oa_bundle).apply().get()
    assert "pdf_path" not in result_non_oa

def test_summarize_from_bundle():
    """Test summarization from either PDF or abstract."""
    # Case 1: Summarize from PDF
    pdf_bundle = {"doi": SAMPLE_DOI, "pdf_path": "/mock/path.pdf"}
    result_pdf = tasks.summarize_from_bundle.s(pdf_bundle).apply().get()
    assert "summary_data" in result_pdf
    assert result_pdf["summary_data"]["based_on"] == "pdf"

    # Case 2: Summarize from abstract
    abstract_bundle = {
        "doi": SAMPLE_DOI,
        "crossref_data": {"abstract": "<p>Html Abstract</p>"}
    }
    result_abstract = tasks.summarize_from_bundle.s(abstract_bundle).apply().get()
    assert "summary_data" in result_abstract
    assert result_abstract["summary_data"]["based_on"] == "abstract"
    assert "Html Abstract" in result_abstract["summary_data"]["summary_short"]

def test_save_to_db():
    """Test the passthrough placeholder for save_to_db."""
    bundle = {"doi": SAMPLE_DOI, "data": "some_data"}
    result = tasks.save_to_db.s(bundle).apply().get()
    assert result == bundle # Should just pass the bundle through

def test_index_search():
    """Test the passthrough placeholder for index_search."""
    bundle = {"doi": SAMPLE_DOI, "data": "some_data"}
    result = tasks.index_search.s(bundle).apply().get()
    assert result["doi"] == SAMPLE_DOI
    assert result["status"] == "indexed"