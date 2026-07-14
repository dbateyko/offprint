# Digital Commons file-access diagnostics

## Current finding

Digital Commons publication HTML and article metadata can remain publicly readable while
`cgi/viewcontent.cgi` returns a generic Bepress HTTP 403 response. This is a shared platform-access
condition, not evidence that an individual journal is paywalled or that its selectors are wrong.
The exact trigger remains unresolved: plausible explanations include platform policy, client or
egress reputation, or traffic-sensitive controls.

The 2026-07-14 Northwestern and William & Mary reconnaissance used correctly scoped URLs with
`context`, `type=pdf`, the article-page Referer, no Range request, and one transparent user agent.
Both hosts returned the same 744-byte HTML denial body and an `original_referer` cookie. No redirect,
login text, or explicit WAF header was present. Historical Northwestern logs show the same URL form
successfully returning PDFs before later requests began receiving 403 responses, so URL shape alone
does not explain the failure.

Local May 2026 logs contain the same broad pattern on at least 24 Digital Commons hosts across 28
run logs. Some runs downloaded valid PDFs before later requests were denied. Treat a bare 403 as
`access_denied`, not as proof of a WAF challenge, login wall, or journal policy.

## Safe diagnostic mode

For a reviewed publication sitemap, the following adapter configuration selects the bounded HTML
enumerator and safe file diagnostic behavior:

```json
{
  "adapter_config": {
    "dc": {
      "enum_mode": "all_issues_only",
      "safe_diagnostic": true
    }
  }
}
```

Safe diagnostic mode forces one request with the project's transparent user agent. It disables TLS
impersonation, user-agent rotation, browser fallback, session-cookie rotation, and retries. On an
unmarked 401/403 it records only non-secret response diagnostics: final URL, status, content type,
body size and SHA-256, server and redirect headers, and cookie names. It never records cookie values.

The `all_issues_only` route now retains the article landing page as `DiscoveryResult.page_url`, so a
later authorized request receives the article—not issue—Referer.

## Decision policy

- Continue publication-scoped metadata enumeration when robots permits it, but record files as
  unavailable until a valid `%PDF-` response is observed.
- Stop the journal scope after the first generic 403 in a diagnostic pass. Do not rotate user agents,
  replay cookies, complete challenges, or launch a browser automatically.
- Use browser interaction only as an explicitly authorized human check. Do not export or replay a
  browser profile or its cookies.
- Ask Bepress/Elsevier or the institution for an approved bulk export, API, or documented automated
  download policy before attempting collection-scale access.
- Recheck `robots.txt` before every smoke or collection run. `/do/` is disallowed on the two pilot
  hosts and must not be used there.

## Remaining implementation work

- Propagate a first generic 403 from the downloader into a scope-level stop signal so discovery does
  not continue queueing known-denied files.
- Add an explicit policy for robots-fetch failures; the current cache is fail-open when the robots
  request itself fails.
- Preserve per-attempt telemetry if multi-profile production mode remains available, so the final
  result cannot hide earlier responses.
- Validate the safe diagnostic from an approved network context before changing either pilot's
  `metadata_only` verdict.
