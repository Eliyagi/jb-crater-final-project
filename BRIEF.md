# Crater — Project Brief

## Business context

Crater is a talent-intelligence product for engineering leaders. The buyers
are VPs of Engineering and senior recruiters at well-funded startups —
companies with twenty to two hundred engineers, hiring two or three senior
ICs a quarter, who have learned the hard way that a polished CV and a
strong interview tell them very little about how someone actually works on
real code over real time. They are willing to pay for *inferred* signals:
what languages someone actually ships in, who they collaborate well with,
whether their open-source activity is sustained or performative, whether
the maintainers they appear next to are the ones doing the work.

The data exhaust that makes this possible is the public GitHub events
firehose — every push, every pull request, every issue comment, every
fork and watch, across every public repository on GitHub, all available
as plain JSON. The raw stream is enormous and polymorphic; the value
Crater sells is the *cleaned, joined, queryable surface* on top of it
that a hiring manager can ask sensible questions of without ever seeing
a JSON object.

Your team's mandate is to build that pipeline and that surface. The
upstream comes from a vendor that mirrors GH Archive (`data.gharchive.org`)
and exposes one gzipped JSONL file per hour at a predictable URL — but
only after that simulated hour has passed, with no listing, no manifest,
and a small catalogue of failure modes you must cope with. You have three
weeks before you defend the system live in class by running five
recruiter-style queries against your own store.

## Data source

The vendor is already running as a containerised application in this
scaffold under `gh-archive-vendor`. There is **no listing endpoint and no
manifest**. Your pipeline must construct the next URL from its own
high-water mark, request it, and act on the response.

**File route.**

```http
GET http://gh-archive-vendor:8000/{YYYY-MM-DD-H}.json.gz
→ 200  gzipped JSONL — every public GitHub event for that simulated hour
→ 404  the simulated clock has not yet reached the end of that hour;
       retry after a backoff
→ 503  the vendor is in a configured outage window; retry after a backoff
```

Note that the hour segment of the filename is **not zero-padded**:
`2024-01-15-3.json.gz` for 03:00, `2024-01-15-13.json.gz` for 13:00. This
matches the real GH Archive convention.

**Operational routes.**

```http
GET /healthz         → {ready, simulated_now, files_available}
GET /simulated_now   → {simulated_now}
GET /stats           → counters + active chaos configuration
```

The simulated clock advances at the rate configured by
`REPLAY_SECONDS_PER_HOUR` (default 2 wall-seconds per simulated hour, so
the full 6-sim-day window completes in roughly five wall-minutes). A file
is only served once its sim-hour has closed; before that you will get 404.

**Event shape.** Each line in the gzipped body is one JSON object with at
minimum these top-level fields:

```
{ "id", "type", "created_at", "actor", "repo", "payload" }
```

`type` is one of approximately fifteen kinds — `PushEvent`,
`PullRequestEvent`, `IssuesEvent`, `IssueCommentEvent`, `WatchEvent`,
`ForkEvent`, `CreateEvent`, `DeleteEvent`, `ReleaseEvent`, and others
that appear less often. The `payload` shape is **different for every
type** — a `PushEvent` payload carries commits and ref information, a
`PullRequestEvent` payload carries the pull request object with its
base/head repositories and merge state, an `IssuesEvent` payload carries
the issue, and so on. Your normalisation step has to handle this
polymorphism, and your storage shape has to support querying across
types.

> **Note on `actor`.** Depending on event type, the `actor` field may be
> the event's initiator (for a `PushEvent`, the person who pushed; for a
> `WatchEvent`, the person who starred) **or** a maintainer who acted on
> someone else's contribution (for a `PullRequestEvent` with
> `action=closed`, the `actor` is whoever closed or merged the PR — not
> necessarily the PR's author, which lives elsewhere in the payload).
> Treating `actor` as "the person who did the underlying work" will give
> you wrong answers on the harder questions below. Decide carefully, per
> event type, who you mean when you say "contributor".

**Unknown event types.** GitHub adds new event types from time to time.
Your pipeline must **capture** events whose type you do not recognise,
not drop them. The intent is that an analyst can answer questions about
those types later without re-fetching history.

**Chaos catalogue.** The vendor simulates each of these on demand (see
`make vendor-chaos`); your system must cope without operator intervention.

- **Slow file.** A served file is throttled to ~50 KB/s, turning a
  sub-second download into 30-60 seconds.
- **Late file.** A file's 200 response is held back past the sim-hour
  boundary for an extra wall-second budget — your probing loop sees 404
  for longer than it expected.
- **Truncated file.** The response is cut after ~70% of bytes. The gzip
  stream ends early; events past the cut are missing until a later
  fetch.
- **Schema drift.** One event in a served file has an extra field in
  `payload` that has never appeared before. Realistic — GitHub adds
  fields over time. Your pipeline must not crash on it.
- **Outage.** Inside a configured sim-time window the vendor returns 503
  for every file. Your retry logic must back off and resume.

## Definition of done

Crater is grading the success of this project around three things: the
five analyst queries below, a written defence of your storage choice,
and an operational baseline they expect of any production-grade system
you hand them.

### Analyst SQL surface — five questions

You will provide **five queries** (one per question below), written
against your own store, and run them live during defence. Each is phrased
as a recruiter-style ask followed by one paragraph of operational
definition so we agree on what is being counted. The queries can be SQL,
Cypher, or whatever language your chosen store speaks — but they must run
end-to-end against your data, against the full replay window, during your
slot.

There is **no dashboard deliverable** in this project. The bar is the
queries themselves and the data model behind them.

**1. What does this person actually code in?**

> For each actor who has merged at least 10 pull requests across the
> replay window, return their top three programming languages by count
> of merged PRs.

A "merged PR" is a `PullRequestEvent` with `action=closed` *and*
`payload.pull_request.merged=true`. The language comes from
`payload.pull_request.base.repo.language` on that same event. The
"author" of the PR is the person who opened it — *not* the actor who
closed it; see the note on `actor` above and dig the author out of the
payload.

**2. Who actually writes the code in this repo?**

> For each of the top 50 repositories in the window by PR activity,
> return the top 5 commit authors and the top 5 pushers, side by side.

A "commit author" is the unique name/email pair on each commit inside
the `payload.commits` array of every `PushEvent` on that repo. A
"pusher" is the `actor` of the `PushEvent`. They are often the same
person but frequently are not — bots, force-pushers, maintainers
applying patches on behalf of others. The comparison is the answer.

**3. Who works well with whom?**

> List the top 10 pairs of developers who have contributed to at least
> 3 distinct repositories together during the window.

"Contributed to a repository" means appearing as either the PR author or
a commit author on that repo. A "pair" is unordered; rank by number of
shared repos descending, break ties by total combined contributions
across those repos.

**4. Does interest convert to contribution?**

> Restrict to repositories that received at least 500 `WatchEvent`s
> (stars) in the window. For each such repo, report what fraction of
> stargazers went on to fork the repo within **2 sim-days** of starring,
> and what fraction of forkers went on to open a pull request within
> **5 sim-days** of forking.

`WatchEvent` is GitHub's "star" event. "Forked the repo" means a
`ForkEvent` by the same actor on the same repo. "Opened a pull request"
means a `PullRequestEvent` with `action=opened` by the same actor
against the same upstream repo. The two-day and five-day windows are
sim-time, tuned to the six-sim-day replay window — you should be able to
compute both with strictly less than the full window of data per actor.

**5. Who's in this person's network?**

> Given a seed actor, return every actor at collaboration distance 1 or
> 2 from them, along with the distance and the repos that connect them.

Two actors are at distance 1 if there exists a repo on which both have
contributed (definition from question 3). Distance 2 follows from one
intermediate hop. The seed actor will be chosen at defence from one of
the top contributors in your data; bring the query parameterised by
actor login.

### Storage-choice rationale

One-page written defence of the storage shape your team picked —
relational, graph, polyglot, or otherwise — and *why*, in light of the
five questions above. We will read it before your defence and ask about
it.

Question 5 is the obvious lever here, but so is question 3 (which can be
done in either shape with very different code) and question 1 (which
wants per-actor aggregates over a typed numeric/categorical column and
strongly favours a columnar layout). There is no single right answer;
there is a right answer *for the trade-offs your team made*. Be ready to
defend it.

### Operational baseline

The system must come up with a single command on a fresh laptop —
`make run` in your project root — and survive a `docker compose restart`
of any one service without losing data, dropping a file you had already
fetched, or requiring a human to babysit it back to a healthy state.

All five chaos modes (slow file, late file, truncated file, schema
drift, outage) must be survivable with chaos toggled on via
`make vendor-chaos`. We will toggle it during your defence.

At your defence, you will provide the five queries above and run them
live against your system. Bring the queries written, prepared to explain
the data model behind them and the pipeline feeding it.
