const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const SOURCE = fs.readFileSync(path.join(__dirname, '..', 'shared-hover-tooltips.js'), 'utf8');

class FakeClassList {
  constructor(initial = []) {
    this.values = new Set(initial);
    this.addCounts = new Map();
  }

  add(...names) {
    names.forEach((name) => {
      this.values.add(name);
      this.addCounts.set(name, (this.addCounts.get(name) || 0) + 1);
    });
  }

  remove(...names) {
    names.forEach((name) => this.values.delete(name));
  }

  contains(name) {
    return this.values.has(name);
  }

  toggle(name, force) {
    const next = force === undefined ? !this.values.has(name) : Boolean(force);
    if (next) this.values.add(name);
    else this.values.delete(name);
    return next;
  }
}

class FakeElement {
  constructor(tagName, options = {}) {
    this.nodeType = 1;
    this.tagName = String(tagName).toUpperCase();
    this.parentElement = null;
    this.children = [];
    this.attributes = new Map();
    this.classList = new FakeClassList(options.classes || []);
    this.style = {};
    this.dataset = {};
    this.textContent = '';
    this.offsetWidth = 120;
    this.offsetHeight = 28;
    this.id = '';
    Object.entries(options.attributes || {}).forEach(([name, value]) => this.setAttribute(name, value));
  }

  appendChild(child) {
    child.parentElement = this;
    this.children.push(child);
    return child;
  }

  setAttribute(name, value) {
    this.attributes.set(name, String(value));
  }

  getAttribute(name) {
    return this.attributes.has(name) ? this.attributes.get(name) : null;
  }

  removeAttribute(name) {
    this.attributes.delete(name);
  }

  matches(selector) {
    if (selector === 'td') return this.tagName === 'TD';
    if (selector === 'table[data-address-match-cells]') {
      return this.tagName === 'TABLE' && this.attributes.has('data-address-match-cells');
    }
    if (selector === '.addr-tooltip-wrap[data-full-addr]') {
      return this.classList.contains('addr-tooltip-wrap') && this.attributes.has('data-full-addr');
    }
    return false;
  }

  closest(selector) {
    let current = this;
    while (current) {
      if (current.matches(selector)) return current;
      current = current.parentElement;
    }
    return null;
  }

  contains(candidate) {
    let current = candidate;
    while (current) {
      if (current === this) return true;
      current = current.parentElement;
    }
    return false;
  }

  querySelectorAll(selector) {
    const matches = [];
    const visit = (node) => {
      node.children.forEach((child) => {
        if (child.matches(selector)) matches.push(child);
        visit(child);
      });
    };
    visit(this);
    return matches;
  }

  querySelector(selector) {
    return this.querySelectorAll(selector)[0] || null;
  }

  getBoundingClientRect() {
    return { left: 100, right: 220, top: 100, bottom: 128, width: 120, height: 28 };
  }
}

class FakeDocument {
  constructor() {
    this.nodeType = 9;
    this.listeners = new Map();
    this.elementsById = new Map();
    this.documentElement = new FakeElement('html');
    this.body = new FakeElement('body');
    this.documentElement.appendChild(this.body);
    const append = this.body.appendChild.bind(this.body);
    this.body.appendChild = (child) => {
      const result = append(child);
      if (child.id) this.elementsById.set(child.id, child);
      return result;
    };
  }

  addEventListener(type, handler) {
    if (!this.listeners.has(type)) this.listeners.set(type, []);
    this.listeners.get(type).push(handler);
  }

  dispatch(type, target, relatedTarget = null) {
    const event = { type, target, relatedTarget, clientX: 110, clientY: 110 };
    (this.listeners.get(type) || []).forEach((handler) => handler(event));
  }

  querySelectorAll() {
    return [];
  }

  querySelector(selector) {
    if (selector === '.addr-tooltip-wrap[data-full-addr]:hover') {
      return this.hoveredAddressTrigger || null;
    }
    return null;
  }

  getElementById(id) {
    return this.elementsById.get(id) || null;
  }

  createElement(tagName) {
    return new FakeElement(tagName);
  }

  elementFromPoint() {
    return null;
  }
}

function appendAddressCell(table, address, options = {}) {
  const row = table.appendChild(new FakeElement('tr'));
  const cell = row.appendChild(new FakeElement('td'));
  let labelTrigger = null;
  if (options.knownLabel) {
    labelTrigger = cell.appendChild(new FakeElement('span', {
      classes: ['addr-tooltip-wrap'],
      attributes: { 'data-full-addr': address },
    }));
    labelTrigger.textContent = 'Wallet';
  }
  const addressTrigger = cell.appendChild(new FakeElement('span', {
    classes: ['addr-tooltip-wrap'],
    attributes: { 'data-full-addr': address },
  }));
  addressTrigger.textContent = /^0x[a-f0-9]{40}$/i.test(address)
    ? `${address.slice(0, 6)}...${address.slice(-4)}`
    : address;
  return { cell, labelTrigger, addressTrigger };
}

function buildFixture(options = {}) {
  const document = new FakeDocument();
  const windowListeners = new Map();
  const window = {
    innerWidth: 1440,
    __DOLO_INLINE_TOOLTIP_ACTIVE: Boolean(options.inlineTooltip),
    addEventListener(type, handler) {
      if (!windowListeners.has(type)) windowListeners.set(type, []);
      windowListeners.get(type).push(handler);
    },
  };
  class FakeMutationObserver {
    constructor(callback) {
      this.callback = callback;
    }
    observe() {}
  }
  const context = vm.createContext({
    document,
    window,
    MutationObserver: FakeMutationObserver,
    setTimeout,
    clearTimeout,
  });
  const address = '0x1111111111111111111111111111111111111111';
  const otherAddress = '0x2222222222222222222222222222222222222222';
  const table = new FakeElement('table', { attributes: { 'data-address-match-cells': '' } });
  const source = appendAddressCell(table, address, { knownLabel: true });
  const peer = appendAddressCell(table, address, { knownLabel: true });
  const duplicateWrapperPeer = appendAddressCell(table, address.toUpperCase().replace('0X', '0x'), { knownLabel: true });
  const other = appendAddressCell(table, otherAddress);
  const malformed = appendAddressCell(table, '0x1234');

  const secondTable = new FakeElement('table', { attributes: { 'data-address-match-cells': '' } });
  const crossTable = appendAddressCell(secondTable, address);

  const unscopedTable = new FakeElement('table');
  const unscoped = appendAddressCell(unscopedTable, address);

  if (options.hoveredAtLoad) document.hoveredAddressTrigger = source.addressTrigger;
  for (let execution = 0; execution < (options.executions || 1); execution += 1) {
    vm.runInContext(SOURCE, context, { filename: 'shared-hover-tooltips.js' });
  }

  return {
    document,
    source,
    peer,
    duplicateWrapperPeer,
    other,
    malformed,
    crossTable,
    unscoped,
  };
}

test('highlights only exact repeated address text within the opted-in table', () => {
  const fixture = buildFixture();

  fixture.document.dispatch('pointerover', fixture.source.addressTrigger);

  assert.equal(fixture.source.addressTrigger.classList.contains('address-match-source'), true);
  assert.equal(fixture.source.addressTrigger.classList.contains('address-match-active'), true);
  assert.equal(fixture.peer.addressTrigger.classList.contains('address-match-peer'), true);
  assert.equal(fixture.peer.addressTrigger.classList.contains('address-match-active'), true);
  assert.equal(fixture.duplicateWrapperPeer.addressTrigger.classList.contains('address-match-peer'), true);
  assert.equal(fixture.source.labelTrigger.classList.contains('address-match-source'), false);
  assert.equal(fixture.peer.labelTrigger.classList.contains('address-match-peer'), false);
  assert.equal(fixture.source.cell.classList.contains('address-match-source'), false);
  assert.equal(fixture.peer.cell.classList.contains('address-match-peer'), false);
  assert.equal(fixture.other.addressTrigger.classList.contains('address-match-peer'), false);
  assert.equal(fixture.crossTable.addressTrigger.classList.contains('address-match-peer'), false);
  assert.equal(fixture.unscoped.addressTrigger.classList.contains('address-match-peer'), false);
});

test('rejects malformed addresses and clears all address text states on pointer exit', () => {
  const fixture = buildFixture();

  fixture.document.dispatch('pointerover', fixture.malformed.addressTrigger);
  assert.equal(fixture.malformed.addressTrigger.classList.contains('address-match-source'), false);

  fixture.document.dispatch('pointerover', fixture.source.addressTrigger);
  assert.equal(fixture.source.addressTrigger.classList.contains('address-match-source'), true);
  assert.equal(fixture.peer.addressTrigger.classList.contains('address-match-peer'), true);
  fixture.document.dispatch('pointerout', fixture.source.addressTrigger);

  assert.equal(fixture.source.addressTrigger.classList.contains('address-match-source'), false);
  assert.equal(fixture.source.addressTrigger.classList.contains('address-match-active'), false);
  assert.equal(fixture.peer.addressTrigger.classList.contains('address-match-peer'), false);
  assert.equal(fixture.duplicateWrapperPeer.addressTrigger.classList.contains('address-match-peer'), false);
});

test('does not emphasize an address that has no visible peer in its table', () => {
  const fixture = buildFixture();

  fixture.document.dispatch('pointerover', fixture.other.addressTrigger);

  assert.equal(fixture.other.addressTrigger.classList.contains('address-match-source'), false);
  assert.equal(fixture.other.addressTrigger.classList.contains('address-match-active'), false);
});

test('uses the same address-text-only state when an address trigger receives focus', () => {
  const fixture = buildFixture();

  fixture.document.dispatch('focusin', fixture.source.addressTrigger);

  assert.equal(fixture.source.addressTrigger.classList.contains('address-match-source'), true);
  assert.equal(fixture.peer.addressTrigger.classList.contains('address-match-peer'), true);
  assert.equal(fixture.other.addressTrigger.classList.contains('address-match-peer'), false);
});

test('keeps address matching active without duplicating an existing page tooltip system', () => {
  const fixture = buildFixture({ inlineTooltip: true });

  fixture.document.dispatch('focusin', fixture.source.addressTrigger);

  assert.equal(fixture.source.addressTrigger.classList.contains('address-match-source'), true);
  assert.equal(fixture.peer.addressTrigger.classList.contains('address-match-peer'), true);
  assert.equal(fixture.document.getElementById('unified-tooltip'), null);
});

test('installs delegated address matching only once when a route reloads the shared asset', () => {
  const fixture = buildFixture({ inlineTooltip: true, executions: 2 });

  assert.equal(fixture.document.listeners.get('pointerover').length, 1);
  fixture.document.dispatch('pointerover', fixture.source.addressTrigger);
  assert.equal(fixture.source.addressTrigger.classList.contains('address-match-source'), true);
});

test('initializes matching when a routed page loads while an address is already hovered', () => {
  const fixture = buildFixture({ inlineTooltip: true, hoveredAtLoad: true });

  assert.equal(fixture.source.addressTrigger.classList.contains('address-match-source'), true);
  assert.equal(fixture.peer.addressTrigger.classList.contains('address-match-peer'), true);
});
