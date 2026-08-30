# Respectful Digital Commons acquisition

> **STALE DIAGNOSIS (2026-08-29).** The WAF analysis below describes AWS WAF
> (`x-amzn-waf-action: challenge`) as observed on 2026-08-02. The platform has
> since moved behind **Cloudflare**, and `/cgi/viewcontent.cgi` now returns a
> hard **403 "Sorry, you have been blocked … unable to access bepress.com"** —
> a firewall block, not a JS challenge a browser resolves by executing script.
> Confirmed on three unrelated institutions. Every HTML surface (landing pages,
> `all_issues.html`, article pages) still returns 200, so enumeration works
> normally; only file retrieval is walled. Last successful bepress PDF anywhere
> in the corpus: **2026-08-06**.
>
> The browser-mediated click-the-Download-link design below is still the right
> architecture and is implemented correctly — it is simply blocked. Because this
> is an access control rather than a challenge, escalating browser realism
> (camoufox, headed Chromium, real-Chrome channel) would be evasion and is out of
> scope by this document's own rules. See `ACQUISITION_BACKLOG_2026-08-29.md` at
> the workspace root.


Status: collector implemented; 100-file browser pilot passed on 2026-08-02.
Permission or a platform export remains a useful optimization, but is not a
prerequisite for collecting public academic repository material.

## Implemented collector and pilot result

The production path is `scripts/pipeline/dc_gather.py`, backed by
`offprint/digital_commons_gather.py`. The queue builder is
`offprint-data-ops/year_resolution/build_dc_gather_queue.py`; its generated
queue lives in `catalog/digital_commons_gather/`.

The 2026-08-02 pilot downloaded exactly 100 unique PDFs (152,911,682 bytes)
through one persistent Chromium process and one global transfer stream. It
rotated fairly across repositories and maintained a ten-second dispatch floor.
There were no HTTP 429, 403, or 503 responses. Every accepted object was
re-read after the run and matched its recorded SHA-256, byte length, and PDF
magic bytes; validation errors were zero.

Four deferred attempt rows were recorded. One was a local Playwright
navigation-wait timeout and succeeded after changing the click trigger; three
were two unique Digital Commons supplemental/native objects that returned HTTP
200 but were not PDFs. The queue builder now excludes the 56 source records
marked `type=additional` instead of repeatedly presenting them as article
PDFs. The rebuilt article queue contains 25,647 objects across 41 domains and
50 publication contexts.

Pilot artifacts are under `artifacts/dc_gather/pilot_20260802/`; the append-only
`attempts.jsonl` is the resume and audit ledger. A bounded run is:

```bash
cd /mnt/shared_storage/law-review-corpus/offprint
python3 scripts/pipeline/dc_gather.py \
  --queue ../catalog/digital_commons_gather/queue.jsonl \
  --out-dir artifacts/dc_gather/pdfs \
  --attempts artifacts/dc_gather/attempts.jsonl \
  --max-items 100 \
  --start-delay-seconds 10 \
  --min-delay-seconds 10 \
  --successes-before-decrease 1000 \
  --contact-email dbateyko@middlebury.edu \
  --project-url https://github.com/dbateyko/offprint \
  --max-download-mib-per-second 5
```

Rerunning the same command is safe: successful gather IDs are skipped, partial
files are removed, and only validated PDFs are atomically promoted.

## Revised operational conclusion

Evidence from the independently operated LRScraper service changes the proposed
strategy. That collector also encounters Digital Commons WAF 403 responses, but
successfully converges by downloading serially, skipping existing files,
continuing past isolated failures, and revisiting gaps in later runs. For
example, its public Brooklyn run history records 258 downloads in 1,171.6
seconds with seven failures, followed by incremental runs that filled more of
the collection. A July run downloaded ten new files without failure in 577.6
seconds, consistent with a substantially slower recent path.

Local diagnostics also distinguish challenge handling from ordinary rate
limiting. A raw request received HTTP 202 with
`x-amzn-waf-action: challenge`. Chromium loaded the article landing page with
HTTP 200, while direct PDF navigation received HTTP 403. The article page's
actual Download anchor did not contain the scraper-added `type=pdf` or
`download=1` parameter. The production collector should therefore click the
publisher-provided link in a persistent browser context instead of first
issuing a cascade of synthetic PDF requests.

The target architecture is a slow, serial, resumable convergence collector:

1. enumerate and deduplicate first;
2. keep one persistent Chromium context for the serial global queue;
3. load the article landing page and click its actual Download link;
4. allow only one Digital Commons PDF transfer globally during the pilot;
5. defer isolated 403/challenge failures and continue with other records;
6. revisit deferred records after host cooldowns and in later waves.

## Finding

The immediate obstacle is not an observed HTTP 429 quota. Historical May 2026
Digital Commons run artifacts contain 92,599 successful PDF responses, 292
actual HTTP 403 responses classified at the time as WAF blocks, and 37,325
additional rows skipped after local host circuit breakers opened. A small
transparent diagnostic on 2026-08-02 received HTTP 403 from PDF endpoints at
several unrelated Digital Commons institutions and no `Retry-After` header.

Because those institutions share Digital Commons infrastructure, hostname
rotation does not create independent capacity. Browser execution of the
platform's ordinary JavaScript and ordinary Download link is acceptable here;
proxy rotation, CAPTCHA solving, fabricated identities, and attempts to defeat
access controls are outside this design.

The public robots files sampled on 2026-08-02 permit ordinary public content and
do not disallow `viewcontent.cgi`, but their wildcard rules disallow `/do/` even
though Digital Commons explicitly documents `/do/oai/` as its supported
outbound OAI-PMH harvesting interface. Resolve that tension by identifying the
harvester transparently, pacing OAI calls conservatively, and caching results.

## Preferred acquisition order

1. **OAI-PMH and existing ledgers for metadata and inventory.** Digital Commons officially exposes
   `oai_dc` records in pages of 100, including landing URLs, direct primary-file
   URLs, dates, creators, source publications, formats, and sometimes rights.
   Harvest publication sets, follow opaque resumption tokens, and use `from`
   dates for later incremental updates. Do not crawl issue and article HTML when
   OAI or a prior ledger already supplies the record.
2. **Browser-mediated, low-rate PDF retrieval.** Visit the OAI-provided landing
   page and click the actual Download anchor in a persistent Chromium context.
   Fetch only objects absent from the content-addressed local inventory.
3. **Institution- or platform-provided export when convenient.** Digital
   Commons documents administrator-facing Content Inventory, API, and bepress
   Archive facilities. These can accelerate stubborn residuals but are not a
   gate on the public-material collector.

## Optional platform coordination

Send Digital Commons Consulting Services a concise request at
`dc-support@elsevier.com` (the address on the official Digital Commons contact
page) containing:

- project identity, public project page, responsible person, and a monitored
  email address;
- noncommercial research purpose and intended preservation/analysis use;
- current scope: 528 registered Digital Commons publication contexts across
  144 hosts, subject to a fresh OAI inventory and local deduplication;
- the fixed source IP, expected date range, and estimated bytes after inventory;
- a request for the preferred bulk-export/API mechanism, whether this IP is
  currently blocked, and an explicit request/second and bandwidth ceiling if
  direct retrieval is acceptable;
- confirmation that the harvester will honor access controls, embargoes,
  withdrawn records, `Retry-After`, and repository-specific rights statements;
- confirmation that download and reuse/redistribution rights will be tracked
  separately.

If Digital Commons cannot grant platform-wide access, approach repository
administrators in institution-sized waves and request Content Inventory or
bepress Archive-derived exports. Do not interpret public download availability
as blanket permission to redistribute PDFs.

Suggested initial message:

> Subject: Request for a sanctioned, low-impact Digital Commons research harvest
>
> We are building a noncommercial research corpus of U.S. law-review scholarship
> and would like to collect public Digital Commons records and primary files
> without burdening the platform or distorting repository usage. Our registry
> currently covers 528 journal publication contexts on 144 Digital Commons
> hosts. We will first deduplicate an OAI inventory against existing holdings,
> so the eventual request will include only missing files.
>
> Could you advise whether a platform export, API, repository-admin export, or
> allowlisted direct-download workflow is preferred? If direct retrieval is
> acceptable, please specify a request-rate and bandwidth ceiling and whether
> our fixed source IP is presently restricted. We will use a truthful user agent
> with monitored contact information, one global queue, resumable downloads,
> and adaptive cooldowns on 403/429/503 responses. We will not use
> proxy rotation, browser/TLS impersonation, or automated challenge solving.
> We will also preserve item-level rights and access metadata and make separate
> redistribution decisions. We are happy to provide the source IP, project URL,
> inventory estimate, and technical plan privately.

## Required polite downloader behavior

The existing downloader is not suitable for this run without a dedicated mode.
Its normal Digital Commons path can impersonate Chrome TLS, rotate among several
user agents, rotate sessions, retry each profile, and use a browser fallback.
A respectful bulk mode must instead enforce all of the following:

- one truthful, stable browser identity with a real project URL and monitored
  contact in the collector metadata;
- browser-first Download-link clicks, not a raw-request/profile cascade followed
  by a disposable browser fallback;
- persistent browser contexts and cookies; replace a context only after a
  cooldown or genuine corruption, not after an arbitrary request count;
- no UA/profile rotation, proxies, CAPTCHA solving, or fabricated identities;
- one platform-global queue across all Digital Commons hostnames, because they
  share infrastructure;
- one in-flight PDF globally for the initial pilot;
- start at one PDF dispatch every 10 seconds; use adaptive multiplicative
  backoff and do not go below five seconds during the pilot;
- one attempt per object per wave, with no immediate profile cascade;
- exact `Retry-After` support for both delta-seconds and HTTP-date formats;
- honor 429/503 `Retry-After`; without one, double the dispatch interval and
  cool the affected host for at least 15 minutes;
- defer an isolated 403 for later retry rather than stopping the corpus; three
  consecutive failures skip that host for the rest of the wave, while
  403/challenge failures on three different hosts in ten minutes stop the whole
  wave; operators must wait at least one hour before resuming that stop;
- OAI/HTML response caching and incremental `from` harvesting;
- SHA-256 and canonical-URL deduplication before transfer, atomic writes, PDF
  validation, and resume checkpoints;
- institution/context round-robin fairness so one large repository cannot
  monopolize the queue;
- a configurable byte-rate ceiling in addition to request pacing.

At one request every 10 seconds, 100,000 missing files require at least 11.6
days before bandwidth, cooldowns, and failures. This is acceptable for a
respectful collection; the exact missing count and byte estimate must be
computed before the full run.

## Staged rollout and stop rules

1. Build an OAI-only inventory and join it to local canonical URL and SHA-256
   holdings. Produce counts and bytes by institution, publication context,
   rights value, and access state.
2. Run 100 missing PDFs spread across at least ten institutions.
3. Review status codes, latency, bytes, PDF validity, rights metadata, and any
   communication from operators.
4. Run a 1,000-object pilot without increasing the validated rate.
5. Expand only if both pilots satisfy the stop rules.

Stop the entire Digital Commons queue when any of these occur:

- a 429 or 503 without a completed `Retry-After` cooldown;
- challenge/403 failures on three different hosts within ten minutes;
- more than 1% HTTP/access failures in a 100-object rolling window;
- a robots/access-policy change or operator request;
- sustained latency or error growth that suggests service pressure.

Persist request timestamp, institution, context, URL, status, redirect target,
response headers relevant to caching/rate limits, byte count, elapsed time,
content hash, attempt count, and stop/cooldown decisions. Never persist
authentication cookies or secrets in run artifacts.

## Release boundary

OAI exposure and public file access answer discovery and retrieval questions;
they do not establish a uniform redistribution license. Preserve `dc:rights`,
license URLs, rights statements, and access conditions per object. Keep the
download inventory separate from the release-eligibility decision.
