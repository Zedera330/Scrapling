from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

import orjson

from scrapling.core.utils import log
from scrapling.core._types import Any, Iterator, Dict, List, Tuple, Union


class ItemList(list):
    """A list of scraped items with export capabilities.
    
    This class extends Python's list to provide convenient export methods
    for scraped data. It supports JSON and JSON Lines formats with
    optimizations for numpy arrays and large datasets.
    
    Examples:
        >>> items = ItemList([{"title": "Example", "price": 10.99}])
        >>> items.to_json("output.json", indent=True)
        >>> items.to_jsonl("output.jsonl")
    """

    def to_json(self, path: Union[str, Path], *, indent: bool = False) -> None:
        """Export items to a JSON file.
        
        The JSON export uses orjson for maximum performance and numpy
        array serialization support. For large datasets, consider using
        to_jsonl() instead as it's more memory-efficient.
        
        :param path: Path to the output file
        :param indent: Pretty-print with 2-space indentation (slightly slower)
        
        Example:
            >>> items.to_json("results.json", indent=True)
        """
        options = orjson.OPT_SERIALIZE_NUMPY
        if indent:
            options |= orjson.OPT_INDENT_2

        file = Path(path)
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(orjson.dumps(list(self), option=options))
        log.info("Saved %d items to %s", len(self), path)

    def to_jsonl(self, path: Union[str, Path]) -> None:
        """Export items as JSON Lines (one JSON object per line).
        
        JSON Lines format is ideal for:
        - Streaming large datasets
        - Append-only operations
        - Line-by-line processing without loading everything into memory
        
        :param path: Path to the output file
        
        Example:
            >>> items.to_jsonl("results.jsonl")
        """
        file_path = Path(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "wb") as f:
            for item in self:
                f.write(orjson.dumps(item, option=orjson.OPT_SERIALIZE_NUMPY))
                f.write(b"\n")
        log.info("Saved %d items to %s", len(self), path)


@dataclass
class CrawlStats:
    """Statistics for a crawl run.
    
    This dataclass tracks all metrics related to a web crawling session,
    including request counts, response times, bandwidth usage, and custom
    statistics. It provides real-time updates during crawling and formatted
    export capabilities.
    
    Attributes:
        requests_count: Total number of requests made
        concurrent_requests: Maximum concurrent requests allowed
        concurrent_requests_per_domain: Maximum concurrent requests per domain
        failed_requests_count: Number of requests that failed
        offsite_requests_count: Requests that went outside allowed domains
        response_bytes: Total bytes downloaded
        items_scraped: Number of successfully scraped items
        items_dropped: Number of items filtered out by pipelines
        start_time: Timestamp when crawl started (time.time())
        end_time: Timestamp when crawl ended
        download_delay: Delay between requests in seconds
        blocked_requests_count: Requests blocked by robots.txt or rules
        custom_stats: User-defined statistics dictionary
        response_status_count: Counter for HTTP status codes
        domains_response_bytes: Bandwidth usage per domain
        sessions_requests_count: Request count per session
        proxies: List of proxies used during crawl
        log_levels_counter: Count of log messages by level
    """

    requests_count: int = 0
    concurrent_requests: int = 0
    concurrent_requests_per_domain: int = 0
    failed_requests_count: int = 0
    offsite_requests_count: int = 0
    response_bytes: int = 0
    items_scraped: int = 0
    items_dropped: int = 0
    start_time: float = 0.0
    end_time: float = 0.0
    download_delay: float = 0.0
    blocked_requests_count: int = 0
    custom_stats: Dict = field(default_factory=dict)
    response_status_count: Dict = field(default_factory=dict)
    domains_response_bytes: Dict = field(default_factory=dict)
    sessions_requests_count: Dict = field(default_factory=dict)
    proxies: List[Union[str, Dict, Tuple]] = field(default_factory=list)
    log_levels_counter: Dict = field(default_factory=dict)

    @property
    def elapsed_seconds(self) -> float:
        """Return total crawl duration in seconds.
        
        :return: End time minus start time, or 0 if start/end not set
        """
        return self.end_time - self.start_time

    @property
    def requests_per_second(self) -> float:
        """Calculate request rate over the crawl duration.
        
        :return: Average requests per second, or 0 if no time elapsed
        """
        if self.elapsed_seconds == 0:
            return 0.0
        return self.requests_count / self.elapsed_seconds

    @property
    def average_response_bytes(self) -> float:
        """Calculate average response size.
        
        :return: Average bytes per response, or 0 if no requests
        """
        if self.requests_count == 0:
            return 0.0
        return self.response_bytes / self.requests_count

    def increment_status(self, status: int) -> None:
        """Increment counter for a specific HTTP status code.
        
        :param status: HTTP status code (e.g., 200, 404, 500)
        
        Example:
            >>> stats.increment_status(200)  # Successful request
            >>> stats.increment_status(404)  # Not found
        """
        self.response_status_count[f"status_{status}"] = self.response_status_count.get(f"status_{status}", 0) + 1

    def increment_response_bytes(self, domain: str, count: int) -> None:
        """Track bandwidth usage per domain and total.
        
        :param domain: Domain name the request was made to
        :param count: Number of bytes in response
        """
        self.response_bytes += count
        self.domains_response_bytes[domain] = self.domains_response_bytes.get(domain, 0) + count

    def increment_requests_count(self, sid: str) -> None:
        """Track requests per session and total.
        
        :param sid: Session identifier string
        """
        self.requests_count += 1
        self.sessions_requests_count[sid] = self.sessions_requests_count.get(sid, 0) + 1

    def to_dict(self) -> dict[str, Any]:
        """Convert statistics to dictionary format for export.
        
        The dictionary includes all crawl metrics with rounded numeric
        values for better readability. This is typically used for
        logging or saving crawl results.
        
        :return: Dictionary containing all statistics
        """
        return {
            "items_scraped": self.items_scraped,
            "items_dropped": self.items_dropped,
            "elapsed_seconds": round(self.elapsed_seconds, 2),
            "download_delay": round(self.download_delay, 2),
            "concurrent_requests": self.concurrent_requests,
            "concurrent_requests_per_domain": self.concurrent_requests_per_domain,
            "requests_count": self.requests_count,
            "requests_per_second": round(self.requests_per_second, 2),
            "average_response_bytes": round(self.average_response_bytes, 2),
            "sessions_requests_count": self.sessions_requests_count,
            "failed_requests_count": self.failed_requests_count,
            "offsite_requests_count": self.offsite_requests_count,
            "blocked_requests_count": self.blocked_requests_count,
            "response_status_count": self.response_status_count,
            "response_bytes": self.response_bytes,
            "domains_response_bytes": self.domains_response_bytes,
            "proxies": self.proxies,
            "custom_stats": self.custom_stats,
            "log_count": self.log_levels_counter,
        }


@dataclass
class CrawlResult:
    """Complete result from a spider run.
    
    This class encapsulates the final output of a crawling session,
    including both the scraped items and comprehensive statistics.
    
    Attributes:
        stats: CrawlStats object with performance metrics
        items: ItemList containing all scraped items
        paused: Whether the crawl was paused before completion
    """

    stats: CrawlStats
    items: ItemList
    paused: bool = False

    @property
    def completed(self) -> bool:
        """Check if crawl completed normally.
        
        :return: True if crawl finished without being paused
        """
        return not self.paused

    def __len__(self) -> int:
        """Return number of scraped items.
        
        :return: Length of items list
        """
        return len(self.items)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """Iterate over scraped items.
        
        :return: Iterator over items dictionary
        """
        return iter(self.items)
