"""Fetchers for system and email sources.

The functions in this module encapsulate the logic required to download
external files.  All network access is delegated here so that it can be
easily mocked or replaced in unit tests.  In the current environment
outbound HTTP and IMAP connections are unavailable, so these functions
contain placeholders where you can insert real implementations once the
credentials and access are configured.
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from datetime import datetime, date
from typing import Iterable, List, Optional, Tuple

import requests  # type: ignore
from app.services.imap_utils import IMAPCompatClient as IMAPClient  # type: ignore

from ..config import get_settings


@dataclass
class DownloadResult:
    """Represents the downloaded contents of a file along with metadata."""

    filename: str
    content: bytes
    sha256: str
    period_from: Optional[date] = None
    period_to: Optional[date] = None


def compute_sha256(data: bytes) -> str:
    """Return the hex digest of a byte string."""
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def download_system_file(campaign_id: int) -> Optional[DownloadResult]:
    """Download the XLSX statistics file for a given campaign.

    The URL is formed by concatenating the base URL from the configuration
    with the campaign ID.  If the request succeeds, a DownloadResult is
    returned.  Otherwise, None is returned.

    NOTE: Outbound HTTP is disabled in the current environment.  This
    function therefore always returns None.  Replace the placeholder with
    `requests.get` logic when running in a real environment.
    """
    settings = get_settings()
    url = f"{settings.system.base_url}/{campaign_id}?export=xlsx"
    try:
        # Perform the HTTP GET request to download the XLSX.  A timeout is
        # supplied to prevent hanging indefinitely.
        response = requests.get(url, timeout=60)
        if response.status_code != 200:
            return None
        content = response.content
        sha = compute_sha256(content)
        filename = f"system_{campaign_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.xlsx"
        return DownloadResult(filename=filename, content=content, sha256=sha)
    except Exception:
        # In case of any network or parsing error, return None.  The caller
        # can decide how to handle the failure (e.g. retry later).
        return None


def fetch_mail_attachments(rule) -> List[DownloadResult]:
    """Fetch attachments from a Yandex mailbox according to a rule.

    Connects to the IMAP server specified in the configuration, selects
    the folder defined by the rule and searches for unseen messages matching
    allowed senders and subject/filename patterns.  Returns a list of
    DownloadResult objects for each attachment.

    NOTE: External IMAP connections are not permitted in this environment.
    To enable real e‑mail processing, uncomment the code and fill in your
    credentials in `config.yaml`.
    """
    settings = get_settings()
    host = settings.imap.host
    port = settings.imap.port
    user = settings.imap.user
    password = settings.imap.password

    # Prepare search criteria based on the rule
    allowed_senders = [s.lower() for s in (rule.allowed_senders or [])]
    # Compile subject and filename regex patterns.  They may be empty lists.
    subject_patterns = [re.compile(p, re.IGNORECASE) for p in (rule.subject_regex or [])]
    filename_patterns = [re.compile(p, re.IGNORECASE) for p in (rule.filename_regex or [])]

    downloads: List[DownloadResult] = []
    try:
        # Connect to the IMAP server.  Use UID mode to simplify flag handling.
        with IMAPClient(host, port=port, use_uid=True, ssl=True) as client:
            client.login(user, password)
            client.select_folder(rule.folder or settings.imap.mailbox)
            # Build search criteria: unseen messages and optional sender filters
            criteria: List[str] = ['UNSEEN']
            if allowed_senders:
                sender_filters = [f'FROM "{s}"' for s in allowed_senders]
                criteria.append('(' + ' OR '.join(sender_filters) + ')')
            message_ids = client.search(criteria)
            if not message_ids:
                return downloads
            # Fetch entire raw emails for each UID
            messages = client.fetch(message_ids, ['RFC822'])
            import email
            for uid, data in messages.items():
                raw = data[b'RFC822']
                msg = email.message_from_bytes(raw)
                sender_addr = email.utils.parseaddr(msg.get('From', ''))[1].lower()
                if allowed_senders and not any(s in sender_addr for s in allowed_senders):
                    continue
                subject = msg.get('Subject', '')
                if subject_patterns and not any(p.search(subject) for p in subject_patterns):
                    continue
                # Iterate over parts to find attachments
                for part in msg.walk():
                    if part.get_content_maintype() == 'multipart':
                        continue
                    disp = part.get('Content-Disposition')
                    if not disp or 'attachment' not in disp.lower():
                        continue
                    filename = part.get_filename()
                    if not filename:
                        continue
                    if filename_patterns and not any(p.search(filename) for p in filename_patterns):
                        continue
                    payload = part.get_payload(decode=True)
                    if not payload:
                        continue
                    sha = compute_sha256(payload)
                    downloads.append(DownloadResult(filename=filename, content=payload, sha256=sha))
                # Mark the message as seen so that it is not processed again
                # Use a double backslash to send the literal "\Seen" flag
                client.add_flags(uid, [b'\\Seen'])
    except Exception:
        # In case of any error (network, auth, parsing), return whatever
        # attachments were successfully processed.  The caller may decide to
        # retry.
        return downloads
    return downloads


