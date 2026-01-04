// test.js - Browser-based tests for dukdb-go WASM filesystem operations

// Global state
let wasmReady = false;

// Console logging helper
function log(type, message, data = null) {
    const consoleEl = document.getElementById('console');
    const entry = document.createElement('div');
    entry.className = `log-entry ${type}`;

    const timestamp = new Date().toLocaleTimeString();
    let text = `[${timestamp}] ${message}`;

    if (data !== null) {
        if (typeof data === 'object') {
            text += '\n' + JSON.stringify(data, null, 2);
        } else {
            text += ' ' + data;
        }
    }

    entry.textContent = text;
    consoleEl.appendChild(entry);
    consoleEl.scrollTop = consoleEl.scrollHeight;
}

// Result display helper
function addResult(testName, success, data) {
    const resultsEl = document.getElementById('results');

    // Remove placeholder if present
    const placeholder = resultsEl.querySelector('p');
    if (placeholder) {
        placeholder.remove();
    }

    const resultEl = document.createElement('div');
    resultEl.className = `result-item ${success ? 'success' : 'error'}`;

    const title = document.createElement('h4');
    title.textContent = `${success ? '\u2705' : '\u274C'} ${testName}`;
    resultEl.appendChild(title);

    const pre = document.createElement('pre');
    pre.textContent = JSON.stringify(data, null, 2);
    resultEl.appendChild(pre);

    resultsEl.appendChild(resultEl);
}

// Clear results
function clearResults() {
    const resultsEl = document.getElementById('results');
    resultsEl.innerHTML = '<p style="color: #6b7280;">Run tests to see results...</p>';
    document.getElementById('console').innerHTML = '<div class="log-entry info">[Init] Console cleared</div>';
    log('info', 'Results and console cleared');
}

// Initialize WASM
async function initWASM() {
    const statusEl = document.getElementById('wasm-status');

    try {
        log('info', 'Initializing Go WASM runtime...');

        // Check if wasm_exec.js loaded properly
        if (typeof Go === 'undefined') {
            throw new Error('wasm_exec.js not loaded - Go class is undefined');
        }

        const go = new Go();

        log('info', 'Fetching dukdb.wasm...');
        const response = await fetch('dukdb.wasm');

        if (!response.ok) {
            throw new Error(`Failed to fetch WASM: ${response.status} ${response.statusText}`);
        }

        const wasmBytes = await response.arrayBuffer();
        log('info', `WASM module loaded: ${(wasmBytes.byteLength / 1024).toFixed(2)} KB`);

        log('info', 'Instantiating WASM module...');
        const result = await WebAssembly.instantiate(wasmBytes, go.importObject);

        log('info', 'Starting Go runtime...');
        go.run(result.instance);

        // Wait a bit for Go to initialize
        await new Promise(resolve => setTimeout(resolve, 100));

        // Check if dukdb namespace was created
        if (typeof window.dukdb === 'undefined') {
            throw new Error('dukdb namespace not created - WASM may not have initialized properly');
        }

        log('success', 'WASM module initialized successfully');
        log('info', `dukdb version: ${window.dukdb.version}`);

        wasmReady = true;
        statusEl.textContent = 'WASM Ready';
        statusEl.className = 'status-badge status-ready';

        // Enable all buttons
        enableButtons(true);

        // Update compatibility grid
        updateCompatibilityGrid();

    } catch (err) {
        log('error', 'Failed to initialize WASM', err.message);
        statusEl.textContent = 'WASM Failed';
        statusEl.className = 'status-badge status-error';

        addResult('WASM Initialization', false, {
            error: err.message,
            hint: 'Make sure dukdb.wasm and wasm_exec.js are in the same directory'
        });
    }
}

// Enable/disable test buttons
function enableButtons(enabled) {
    const buttons = [
        'btn-run-all',
        'btn-http-read',
        'btn-http-stat',
        'btn-fetch',
        'btn-filesystem',
        'btn-range'
    ];

    buttons.forEach(id => {
        const btn = document.getElementById(id);
        if (btn) {
            btn.disabled = !enabled;
        }
    });
}

// Update compatibility grid
function updateCompatibilityGrid() {
    if (!wasmReady) return;

    const gridEl = document.getElementById('compatibility-grid');
    gridEl.innerHTML = '';

    const schemes = [
        { name: 'http', label: 'HTTP' },
        { name: 'https', label: 'HTTPS' },
        { name: 's3', label: 'S3' },
        { name: 'gs', label: 'GCS' },
        { name: 'azure', label: 'Azure' },
        { name: 'file', label: 'Local' }
    ];

    schemes.forEach(scheme => {
        const isSupported = window.dukdb.isWASMCompatible(scheme.name);

        const item = document.createElement('div');
        item.className = `compat-item ${isSupported ? 'supported' : 'unsupported'}`;

        item.innerHTML = `
            <div class="scheme">${scheme.label}</div>
            <div class="status">${isSupported ? 'Supported' : 'Use pre-signed URLs'}</div>
        `;

        gridEl.appendChild(item);
    });
}

// Get test URL from input
function getTestURL() {
    return document.getElementById('test-url').value.trim();
}

// Test HTTP Read
async function testHTTPRead() {
    if (!wasmReady) {
        log('error', 'WASM not ready');
        return;
    }

    const url = getTestURL();
    log('info', 'Testing HTTP Read...', url);

    try {
        const result = await window.dukdb.testHTTPRead(url);
        log('success', 'HTTP Read completed', result);
        addResult('HTTP Read', result.success, result);
    } catch (err) {
        log('error', 'HTTP Read failed', err.message);
        addResult('HTTP Read', false, { error: err.message });
    }
}

// Test HTTP Stat
async function testHTTPStat() {
    if (!wasmReady) {
        log('error', 'WASM not ready');
        return;
    }

    const url = getTestURL();
    log('info', 'Testing HTTP Stat...', url);

    try {
        const result = await window.dukdb.testHTTPStat(url);
        log('success', 'HTTP Stat completed', result);
        addResult('HTTP Stat', result.success, result);
    } catch (err) {
        log('error', 'HTTP Stat failed', err.message);
        addResult('HTTP Stat', false, { error: err.message });
    }
}

// Test Fetch API
async function testFetch() {
    if (!wasmReady) {
        log('error', 'WASM not ready');
        return;
    }

    const url = getTestURL();
    log('info', 'Testing Fetch API...', url);

    try {
        const result = await window.dukdb.testFetch(url);
        log('success', 'Fetch completed', result);
        addResult('Fetch API', result.success, result);
    } catch (err) {
        log('error', 'Fetch failed', err.message);
        addResult('Fetch API', false, { error: err.message });
    }
}

// Test FileSystem Factory
async function testFileSystem() {
    if (!wasmReady) {
        log('error', 'WASM not ready');
        return;
    }

    const url = getTestURL();
    log('info', 'Testing FileSystem Factory...', url);

    try {
        const result = await window.dukdb.testFileSystemFactory(url);
        log('success', 'FileSystem Factory test completed', result);
        addResult('FileSystem Factory', result.success, result);
    } catch (err) {
        log('error', 'FileSystem Factory test failed', err.message);
        addResult('FileSystem Factory', false, { error: err.message });
    }
}

// Test Range Read
async function testRangeRead() {
    if (!wasmReady) {
        log('error', 'WASM not ready');
        return;
    }

    const url = getTestURL();
    log('info', 'Testing Range Read...', url);

    try {
        // Read bytes 0-99
        const result = await window.dukdb.testHTTPRangeRead(url, 0, 100);
        log('success', 'Range Read completed', result);
        addResult('Range Read (0-100)', result.success, result);
    } catch (err) {
        log('error', 'Range Read failed', err.message);
        addResult('Range Read', false, { error: err.message });
    }
}

// Run all tests
async function runAllTests() {
    if (!wasmReady) {
        log('error', 'WASM not ready');
        return;
    }

    const url = getTestURL();
    log('info', 'Running all tests...', url);

    // Clear previous results
    clearResults();

    try {
        // Use the built-in runAllTests function
        const result = await window.dukdb.runAllTests(url);
        log('success', 'All tests completed', result);

        // Add individual results
        if (result.results) {
            Object.entries(result.results).forEach(([testName, testResult]) => {
                const success = testResult.success !== false;
                addResult(testName, success, testResult);
            });
        }

        addResult('Summary', result.success, {
            testURL: result.testURL,
            totalTests: Object.keys(result.results || {}).length
        });

    } catch (err) {
        log('error', 'runAllTests failed', err.message);
        addResult('All Tests', false, { error: err.message });
    }

    // Also run individual tests
    log('info', 'Running individual tests...');

    await testHTTPStat();
    await testHTTPRead();
    await testFetch();
    await testFileSystem();
    await testRangeRead();

    log('success', 'All individual tests completed');
}

// Test async filesystem API
async function testAsyncFileSystem() {
    if (!wasmReady) {
        log('error', 'WASM not ready');
        return;
    }

    const url = getTestURL();
    log('info', 'Testing Async FileSystem API...', url);

    try {
        // Create HTTP filesystem
        const fs = await window.dukdb.createHTTPFileSystem();
        log('info', 'Created HTTP filesystem');

        // Test exists
        const exists = await fs.exists(url);
        log('info', `File exists: ${exists}`);

        // Test stat
        const stat = await fs.stat(url);
        log('info', 'File stat', stat);

        // Test open and read
        const file = await fs.open(url);
        log('info', 'Opened file');

        const data = await file.read(1024);
        log('info', `Read ${data.length} bytes`);

        // Get file stat
        const fileStat = await file.stat();
        log('info', 'File stat from handle', fileStat);

        await file.close();
        log('info', 'Closed file');

        addResult('Async FileSystem API', true, {
            exists: exists,
            stat: stat,
            bytesRead: data.length
        });

    } catch (err) {
        log('error', 'Async FileSystem test failed', err.message);
        addResult('Async FileSystem API', false, { error: err.message });
    }
}

// Initialize when page loads
document.addEventListener('DOMContentLoaded', function() {
    log('info', 'Page loaded, initializing...');
    initWASM();
});

// Export functions to global scope for button onclick handlers
window.testHTTPRead = testHTTPRead;
window.testHTTPStat = testHTTPStat;
window.testFetch = testFetch;
window.testFileSystem = testFileSystem;
window.testRangeRead = testRangeRead;
window.runAllTests = runAllTests;
window.clearResults = clearResults;
window.testAsyncFileSystem = testAsyncFileSystem;
