(function (root, factory) {
    const api = factory();
    if (typeof module === 'object' && module.exports) module.exports = api;
    if (root) root.EarnRpcPolicy = api;
}(typeof globalThis !== 'undefined' ? globalThis : this, function () {
    'use strict';

    function getHttpStatus(error) {
        const direct = Number(error && (error.httpStatus || error.status));
        if (Number.isInteger(direct) && direct > 0) return direct;
        const match = String((error && error.message) || error || '').match(/(?:rpc\s+http|http)\s+(\d{3})/i);
        return match ? Number(match[1]) : 0;
    }

    function classify(error) {
        const message = String((error && error.message) || error || '').toLowerCase();
        const httpStatus = getHttpStatus(error);
        const rpcCode = Number(error && error.rpcCode);
        if (httpStatus === 401 || httpStatus === 403) {
            return { kind: 'endpoint_fatal', retryable: true, httpStatus };
        }
        if (
            message.includes('archive requests require') ||
            message.includes('personal token') ||
            message.includes('usage limit for your current plan')
        ) {
            return { kind: 'endpoint_fatal', retryable: true, httpStatus };
        }
        if (
            message.includes('metadata is not found') ||
            message.includes('account metadata not found') ||
            message.includes('invalid account number')
        ) {
            return { kind: 'request_fatal', retryable: false, httpStatus };
        }
        if (
            httpStatus === 429 ||
            httpStatus >= 500 ||
            rpcCode === -32603 ||
            message.includes('rate limit') ||
            message.includes('too many requests') ||
            message.includes('temporary internal error') ||
            message.includes('abort') ||
            message.includes('timeout') ||
            message.includes('failed to fetch') ||
            message.includes('networkerror')
        ) {
            return { kind: 'retryable', retryable: true, httpStatus };
        }
        return { kind: 'request_fatal', retryable: false, httpStatus };
    }

    function create(endpoints, options) {
        const urls = Array.from(new Set((endpoints || []).map(String).filter(Boolean)));
        const now = options && typeof options.now === 'function' ? options.now : () => Date.now();
        const states = new Map();
        let cursor = 0;

        function stateFor(url) {
            if (!states.has(url)) states.set(url, { disabled: false, retryAfter: 0 });
            return states.get(url);
        }

        function next() {
            if (urls.length === 0) return null;
            const currentTime = now();
            for (let offset = 0; offset < urls.length; offset++) {
                const index = (cursor + offset) % urls.length;
                const url = urls[index];
                const state = stateFor(url);
                if (state.disabled || state.retryAfter > currentTime) continue;
                cursor = (index + 1) % urls.length;
                return url;
            }
            return null;
        }

        function recordFailure(url, error) {
            const result = classify(error);
            const state = stateFor(url);
            if (result.kind === 'endpoint_fatal') {
                state.disabled = true;
                state.retryAfter = 0;
            } else if (result.kind === 'retryable') {
                const delay = result.httpStatus === 429 ? 1500 : 400;
                state.retryAfter = now() + delay;
            }
            return result;
        }

        function recordSuccess(url) {
            const state = stateFor(url);
            state.retryAfter = 0;
        }

        function reset() {
            states.clear();
            cursor = 0;
        }

        return {
            classify,
            next,
            recordFailure,
            recordSuccess,
            reset,
            shouldRetry: error => classify(error).retryable,
        };
    }

    return { classify, create };
}));
