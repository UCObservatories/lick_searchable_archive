import logging
from datetime import datetime, date

import requests

from tenacity import Retrying, stop_after_delay, wait_exponential

logger = logging.getLogger(__name__)


class LickArchiveClient:
    """Client for the Lick Searchable Archive's REST API
    
    Args:
    archive_url (str):             The URL of the Archive REST API
    retry_max_delay (int):         The maximum delay between retrying an API call in seconds. 
                                   The actual delay will be an exponential backoff starting at 5s. 
    retry_max_time (int):          The maximum time to spend retrying a call.
    request_timeout (int):         The maximum time to wait for an API call to return before
                                   timing out and assuming it failed.
    ssl_verify (str):              Optional. Path to a public key or CA bundle for SSL vberification.
    
    """
    def __init__(self, archive_url, retry_max_delay, retry_max_time, request_timeout, ssl_verify=None):
    
        # The ingest URLs should have a / on it so the sync_query, or ingest_new_files part can be appended
        if archive_url[-1] == '/':
            self.ingest_url = archive_url
        else:
            self.ingest_url = archive_url + '/'

        self.retry_max_delay = retry_max_delay
        self.retry_max_time = retry_max_time
        self.request_timeout = request_timeout
        self.ssl_verify = ssl_verify

    def query(self, field, value, contains=False, match_case=None, prefix=False, count=False, results=["filename"], sort=None, page=1, page_size=50):
        """
        Find the files in the archive that match a query.

            field (str): The field to query on. "filename", "object", "date", and "datetime" are the only accepted fields currently.
            value (Any): The value being queried on. This depends on the field being queried:
                         "filename", "object": A string 
                         "date": A datetime.date object or a sequence of two datetime.date objects. One date is for an exact match and two for the start and end of a date range.
                         "datetime": A datetime.datetime object or a sequence of two datetime.datetime objects. One date is for an exact match and two for the start and end of a date range.
            contains (bool): Whether a string query should query for a substring or an exact match. Defaults to False. Has no effect for date queries.
            match_case (bool): Whether a string query should be case sensitive. Only applicable to object searches.
            prefix (bool): Whether a string query should query for the prefix or an exact match. Defaults to False. Has no effect for date queries.
            count (int): Whether to return a count of how many files match the query instead of the metadata from the files. Defaults to False.
            results (list of str): The list of metadata attributes to return. Defaults to ["filename"]. This is ignored
                                   if count is True.
            sort (list of str): The list of metadata attributes to sort by. Prefix an attribute with "-" for a
                                descending sort. Defaults to ["id"].
            page_size (int):    How many items to return per page. Defaults to 50.
        Return:
            If count is true:
                int : The number of files that match the query
            Else:
                list: A list of dict objects containing the resulting metadata. The keys are the attributes provided in results.
                str: The URL to the previous page of results. None if there is no previous page.
                str: The URL to the next page of results. None if there is no next page.
                int: The total number of results the query matches.

        Raises:
            requests.RequestException on failure contacting the archive server.
            ValueError If an invalid result is returned from the archive server.
        """
        # Validate the field being queried on 
        if field not in ["filename", "object", "date", "datetime"]:
            raise ValueError(f"Unknown query field '{field}'")

        # Build query parameters
        if field == "date" or field=="datetime":
            # Convert the date range tuple to a comma separated list
            if isinstance(value, datetime) or isinstance(value,date):
                query_params = {field: str(value)}
            else:
                query_params = {field: ",".join([str(date_value) for date_value in value])}
        else:
            query_params = {field: str(value)}

        if prefix is True:
            query_params["prefix"] = True
        elif contains is True:
            query_params["contains"] = True
        
        if match_case is not None:
            query_params["match_case"] = match_case

        if count is True:
            query_params["count"] = True
        else:
            query_params["results"] = ",".join(results)
            query_params["page_size"] = page_size
            query_params["page"]=page
            if sort is not None:
                if not isinstance(sort, list):
                    sort = [sort]
                if len(sort) !=0:
                    query_params["sort"] = ",".join(sort)

        logger.debug(f"Querying archive: url:{self.ingest_url} params: {query_params}")        

        return self._process_results(self._run_query(query_params), count)

    def _run_query(self, query_params):
        """Helper method to send the query request to the archive server.
        
        Args:
            query_params (dict): The query parameters to send.
        
        Return:

        """
        # We run the request using slightly over the TCP timeout of 3 seconds for the socket connect.
        # The request_timeout is the timeout between bytes sent from the server
        logger.debug(f"Querying archive: url:{self.ingest_url} params: {query_params}")
        retryer = Retrying(stop=stop_after_delay(self.retry_max_time), wait=wait_exponential(multiplier=1, min=5, max=self.retry_max_delay))
        result = retryer(requests.get, self.ingest_url + "data/", params=query_params, verify=self.ssl_verify, timeout=(3.1, self.request_timeout))
        result.raise_for_status()

        return result.json()


    def _process_results(self, result_json, count=False):
        logger.debug(f"Results from archive: {result_json}")

        # Get the number of results from the query as an integer
        if 'count' in result_json:
            query_count = int(result_json['count'])
        else:
            raise ValueError("Archive server did not return an count value for a query requesting a count.")
        
        if count:
            # The count was all that was requested
            return query_count, None, None, None
        else:
            # Return a list of results for non-count queries
            if 'results' in result_json:
                # Get next/previous page links
                if 'next' in result_json:
                    next_page = result_json['next']
                else:
                    next_page = None

                if 'previous' in result_json:
                    prev_page = result_json['previous']
                else:
                    prev_page = None

                return query_count, result_json['results'], prev_page, next_page
            else:
                raise ValueError("Archive server did not return results for a query.")
        
