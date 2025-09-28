import yaml
import requests
import logging
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

from .celery_app import app

# Configure logger
logger = logging.getLogger(__name__)

# --- Helper Functions ---
def load_topics(file_path='topics.yml'):
    """Loads topics and keywords from the YAML file."""
    try:
        with open(file_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Topics file not found at {file_path}")
        return {}
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file {file_path}: {e}")
        return {}

def build_openalex_filter(topics, from_date=None, to_date=None, updated_since=None):
    """Builds the filter part of the OpenAlex API query."""
    all_includes = []
    all_excludes = []
    for category in topics.values():
        all_includes.extend(category.get('include', []))
        all_excludes.extend(category.get('exclude', []))

    include_str = ",".join([f"display_name.search:{term}" for term in all_includes])
    exclude_str = ",".join([f"-display_name.search:{term}" for term in all_excludes])

    filters = [f for f in [include_str, exclude_str] if f]

    if updated_since:
        filters.append(f"updated_date:>{updated_since}")

    if from_date:
        filters.append(f"from_publication_date:{from_date}")
    if to_date:
        filters.append(f"to_publication_date:{to_date}")

    return ",".join(filters) if filters else ""


# --- Celery Tasks (Refactored for data bundle pattern) ---

@app.task(
    autoretry_for=(requests.exceptions.RequestException,),
    retry_kwargs={'max_retries': 3},
    bind=True
)
def scan_openalex(self, updated_since=None):
    """
    Scans OpenAlex for new/updated works based on topics.
    Returns a list of DOIs.
    """
    logger.info("Starting OpenAlex scan...")
    if not updated_since:
        updated_since = (datetime.utcnow() - timedelta(hours=24)).isoformat()

    topics = load_topics()
    if not topics:
        logger.warning("No topics loaded, skipping scan.")
        return []

    base_url = "https://api.openalex.org/works"
    filters = build_openalex_filter(topics, updated_since=updated_since)

    params = {'filter': filters, 'per_page': 200, 'mailto': 'jules.agent@example.com'}
    dois = []
    page = 1

    # For testing, let's limit to a small number of results
    max_dois = 5

    while len(dois) < max_dois:
        try:
            params['page'] = page
            logger.info(f"Fetching page {page} with filters: {filters}")
            response = requests.get(base_url, params=params)
            response.raise_for_status()

            data = response.json()
            results = data.get('results', [])

            if not results:
                logger.info("No more results from OpenAlex.")
                break

            for work in results:
                if work.get('doi'):
                    doi = work['doi'].replace("https://doi.org/", "")
                    if doi not in dois:
                        dois.append(doi)
                        if len(dois) >= max_dois:
                            break

            page += 1

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logger.warning("Rate limit exceeded. Retrying...")
                self.retry(exc=e, countdown=60)
            else:
                logger.error(f"HTTP Error: {e}")
                raise
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            raise

    logger.info(f"Scan complete. Found {len(dois)} new/updated works.")
    return dois

@app.task(
    autoretry_for=(requests.exceptions.RequestException,),
    retry_kwargs={'max_retries': 3},
    bind=True
)
def scan_openalex_by_date_range(self, from_date, to_date, max_results=5000):
    """
    Scans OpenAlex for works within a specific date range for backfilling.
    """
    logger.info(f"Starting OpenAlex backfill scan for {from_date} to {to_date}...")

    topics = load_topics()
    if not topics:
        logger.warning("No topics loaded, skipping scan.")
        return []

    base_url = "https://api.openalex.org/works"
    filters = build_openalex_filter(topics, from_date=from_date, to_date=to_date)

    params = {'filter': filters, 'per_page': 200, 'mailto': 'jules.agent@example.com'}
    dois = []
    page = 1

    while len(dois) < max_results:
        try:
            params['page'] = page
            logger.info(f"Fetching page {page} with filters: {filters}")
            response = requests.get(base_url, params=params)
            response.raise_for_status()

            data = response.json()
            results = data.get('results', [])

            if not results:
                logger.info("No more results from OpenAlex for this date range.")
                break

            for work in results:
                if work.get('doi'):
                    doi = work['doi'].replace("https://doi.org/", "")
                    if doi not in dois:
                        dois.append(doi)
                        if len(dois) >= max_results:
                            break

            page += 1

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logger.warning("Rate limit exceeded during backfill. Retrying...")
                self.retry(exc=e, countdown=120) # Longer countdown for backfill
            else:
                logger.error(f"HTTP Error during backfill: {e}")
                raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during backfill: {e}")
            raise

    logger.info(f"Backfill scan complete. Found {len(dois)} works.")
    return dois


@app.task(
    autoretry_for=(requests.exceptions.RequestException,),
    retry_kwargs={'max_retries': 3, 'countdown': 5}
)
def enrich_crossref(doi):
    """Enriches metadata and creates the initial data bundle."""
    logger.info(f"Enriching DOI: {doi} via Crossref")
    bundle = {'doi': doi}
    url = f"https://api.crossref.org/works/{doi}"
    headers = {'mailto': 'jules.agent@example.com'}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        bundle['crossref_data'] = response.json().get('message', {})
        logger.info(f"Successfully enriched DOI: {doi}")
        return bundle
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error enriching DOI {doi}: {e}")
        raise

@app.task(
    autoretry_for=(requests.exceptions.RequestException,),
    retry_kwargs={'max_retries': 3, 'countdown': 5}
)
def classify_oa(bundle):
    """Classifies OA status using the Unpaywall API and adds to bundle."""
    doi = bundle['doi']
    logger.info(f"Classifying OA status for DOI: {doi} via Unpaywall")
    url = f"https://api.unpaywall.org/v2/{doi}"
    params = {'email': 'jules.agent@example.com'}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        bundle['oa_data'] = {
            'is_oa': data.get('is_oa', False),
            'oa_status': data.get('oa_status'),
            'license': data.get('license'),
            'best_oa_location': data.get('best_oa_location') # Get best location
        }
        logger.info(f"Successfully classified OA status for DOI: {doi}")
        return bundle
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error classifying OA for DOI {doi}: {e}")
        raise

@app.task
def fetch_pdf_if_oa(bundle):
    """Fetches PDF if the work is Open Access and adds path to bundle."""
    doi = bundle['doi']
    is_oa = bundle.get('oa_data', {}).get('is_oa', False)

    if is_oa:
        best_location = bundle.get('oa_data', {}).get('best_oa_location')
        if best_location and best_location.get('url_for_pdf'):
            pdf_url = best_location['url_for_pdf']
            logger.info(f"Found OA PDF for DOI: {doi} at {pdf_url}. Simulating download.")
            # In a real implementation, download the PDF here.
            # For now, we just record a mock path.
            bundle['pdf_path'] = f'/tmp/{doi.replace("/", "_")}.pdf'
        else:
            logger.warning(f"Work {doi} is OA, but no PDF URL found in best_oa_location.")
    else:
        logger.info(f"Work {doi} is not Open Access. Skipping PDF fetch.")

    return bundle

def clean_abstract(html_abstract):
    """Removes HTML tags from the abstract text."""
    if not html_abstract:
        return ""
    soup = BeautifulSoup(html_abstract, "html.parser")
    return soup.get_text()

@app.task
def summarize_from_bundle(bundle):
    """Summarizes work based on PDF (if available) or abstract."""
    doi = bundle['doi']
    logger.info(f"Starting summarization for DOI: {doi}")

    if bundle.get('pdf_path'):
        # Mock reading PDF content
        text_content = "This is mock content from the downloaded PDF."
        based_on = "pdf"
    else:
        abstract = bundle.get('crossref_data', {}).get('abstract', '')
        text_content = clean_abstract(abstract)
        based_on = "abstract"

    if not text_content:
        logger.warning(f"No content (PDF or abstract) to summarize for DOI: {doi}")
        bundle['summary_data'] = None
        return bundle

    # Mock summary generation
    summary_short = f"This is a mock summary based on {based_on}. " + text_content[:100] + "..."
    key_claims = ["Mock key claim 1.", "Mock key claim 2."]

    bundle['summary_data'] = {
        'summary_short': summary_short, 'key_claims': key_claims,
        'model_version': 'mock_v1', 'based_on': based_on, 'confidence': 0.95
    }
    logger.info(f"Successfully summarized DOI: {doi} based on {based_on}")
    return bundle

@app.task
def save_to_db(bundle):
    """Placeholder for saving all collected data to the database."""
    doi = bundle['doi']
    logger.info(f"Saving data bundle for DOI: {doi} to database.")
    # In a real implementation, you would use models.py to save:
    # bundle['crossref_data'] -> works table
    # bundle['oa_data'] -> access table
    # bundle['summary_data'] -> summaries table
    logger.info("Data saved successfully (mock).")
    return bundle

@app.task
def index_search(bundle):
    """Placeholder for indexing data into a search engine."""
    doi = bundle.get('doi', 'N/A')
    logger.info(f"Indexing data for DOI: {doi} into search engine.")
    return {'doi': doi, 'status': 'indexed'}