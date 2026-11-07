#!/usr/bin/env python3
"""Bounded recovery for Pages publication, never DOLO flow/holder collectors.

Pages publishes master under its existing serialized/coalescing concurrency
policy. Repeating it does not rerun the DOLO flow or holder collectors.
Reconcile an ambiguous API failure before retrying; never hide final failure.
"""
import argparse
import json
import os
import re
import subprocess
import time

SHA = re.compile(r'^[0-9a-f]{40}$', re.I)
REPOSITORY = re.compile(r'^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$')
TRANSIENT = {408, 429, 500, 502, 503, 504}
ACTIVE = {'queued', 'in_progress', 'requested', 'waiting', 'pending'}


class DispatchError(Exception):
    pass


def command(args, runner):
    return runner(['gh'] + args, capture_output=True, text=True, timeout=30)


def api(path, runner):
    try:
        result = command(['api', path], runner)
    except (OSError, subprocess.TimeoutExpired):
        raise DispatchError('metadata_unavailable') from None
    if result.returncode:
        raise DispatchError('metadata_unavailable')
    try:
        return json.loads(result.stdout)
    except (TypeError, ValueError):
        raise DispatchError('metadata_invalid') from None


def covering_run(repo, commit, runner):
    base = 'repos/' + repo
    payload = api(base + '/actions/workflows/pages.yml/runs?branch=master&per_page=10', runner)
    if not isinstance(payload, dict) or not isinstance(payload.get('workflow_runs'), list):
        raise DispatchError('metadata_invalid')
    comparisons = 0
    for run in payload['workflow_runs']:
        if not isinstance(run, dict):
            raise DispatchError('metadata_invalid')
        if (not isinstance(run.get('head_branch'), str) or not run['head_branch']
                or run.get('status') not in ACTIVE | {'completed'}):
            raise DispatchError('metadata_invalid')
        if run.get('head_branch') != 'master':
            continue
        if run.get('status') not in ACTIVE and not (
                run.get('status') == 'completed' and run.get('conclusion') == 'success'):
            continue
        head = run.get('head_sha')
        if type(run.get('id')) is not int or run['id'] <= 0 or not isinstance(head, str) or not SHA.fullmatch(head):
            raise DispatchError('metadata_invalid')
        if head.lower() == commit.lower():
            return run['id']
        # A later run can include the publication after concurrent bot commits.
        # Keep metadata lookups bounded even if the history contains many runs.
        if comparisons >= 2:
            continue
        comparisons += 1
        comparison = api(base + '/compare/' + commit + '...' + head, runner)
        if (not isinstance(comparison, dict)
                or comparison.get('status') not in {'ahead', 'behind', 'diverged', 'identical'}
                or not isinstance(comparison.get('merge_base_commit'), dict)
                or not isinstance(comparison['merge_base_commit'].get('sha'), str)
                or not SHA.fullmatch(comparison['merge_base_commit']['sha'])):
            raise DispatchError('metadata_invalid')
        if (comparison.get('status') in ('ahead', 'identical')
                and comparison['merge_base_commit'].get('sha') == commit):
            return run['id']
    return None


def emit(action, **fields):
    print(json.dumps(dict(action=action, **fields), sort_keys=True))


def main(argv=None, *, runner=subprocess.run, sleep=time.sleep):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', default=os.environ.get('GITHUB_REPOSITORY', ''))
    parser.add_argument('--commit', required=True)
    args = parser.parse_args(argv)
    if not REPOSITORY.fullmatch(args.repo) or not SHA.fullmatch(args.commit):
        emit('failed', reason='invalid_target')
        return 1
    for attempt in range(1, 4):
        try:
            result = command(['workflow', 'run', 'pages.yml', '--repo', args.repo, '--ref', 'master'], runner)
        except subprocess.TimeoutExpired:
            status = None
        except OSError:
            emit('failed', reason='gh_unavailable', attempts=attempt)
            return 1
        else:
            if result.returncode == 0:
                emit('dispatched', attempts=attempt)
                return 0
            match = re.search(r'\bHTTP\s+(\d{3})\b', result.stderr or '')
            status = int(match.group(1)) if match else None
            if status not in TRANSIENT:
                emit('failed', reason='dispatch_rejected', status=status, attempts=attempt)
                return 1
        # Do not echo CLI error bodies, URLs or credentials into logs.
        emit('retry_check', attempts=attempt, status=status)
        sleep(2 * attempt)
        try:
            existing = covering_run(args.repo, args.commit, runner)
        except DispatchError as error:
            emit('failed', reason=str(error), attempts=attempt)
            return 1
        if existing is not None:
            emit('already_requested', runId=existing, attempts=attempt)
            return 0
    emit('failed', reason='retry_exhausted', attempts=3)
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
