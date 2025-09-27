import logging
from celery import chain, group
from .celery_app import app
from .tasks import (
    scan_openalex,
    enrich_crossref,
    classify_oa,
    fetch_pdf_if_oa,
    summarize_from_bundle,
    save_to_db,
    index_search,
)

logger = logging.getLogger(__name__)

@app.task(name="pipelines.start_harvest")
def start_harvest():
    """
    The main entry point for the incremental harvesting pipeline.
    It scans for new works and then triggers the processing pipeline for each.
    """
    logger.info("Starting the incremental harvest pipeline.")
    # Create a pipeline: scan for DOIs, then process the resulting list.
    pipeline = (scan_openalex.s() | process_dois.s())
    pipeline.apply_async()
    logger.info("Incremental harvest pipeline has been triggered.")

@app.task(name="pipelines.backfill_batch")
def backfill_batch(year, max_results_per_batch=5000):
    """
    Entry point for a backfill batch for a specific year.
    """
    logger.info(f"Starting backfill batch for the year {year}.")
    from_date = f"{year}-01-01"
    to_date = f"{year}-12-31"

    # Create a backfill pipeline: scan by date range, then process the results.
    pipeline = (
        scan_openalex_by_date_range.s(
            from_date=from_date,
            to_date=to_date,
            max_results=max_results_per_batch
        ) | process_dois.s()
    )
    pipeline.apply_async()
    logger.info(f"Backfill batch for {year} has been triggered.")

@app.task(name="pipelines.process_dois")
def process_dois(dois):
    """
    Takes a list of DOIs and creates a processing group.
    Each DOI gets its own processing chain.
    """
    if not dois:
        logger.info("No new DOIs to process.")
        return

    logger.info(f"Creating processing group for {len(dois)} DOIs.")
    # A group executes a list of tasks in parallel.
    # Each task in the list is a chain for processing a single DOI.
    processing_group = group(
        chain(
            enrich_crossref.s(doi),
            classify_oa.s(),
            fetch_pdf_if_oa.s(),
            summarize_from_bundle.s(),
            save_to_db.s(),
            index_search.s()
        )
        for doi in dois
    )
    # Execute the group of chains.
    processing_group.apply_async()
    logger.info(f"Group of {len(dois)} processing chains has been triggered.")