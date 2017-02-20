let fs = require('fs-es6-promise');
let request = require('request-promise-native');

const max_cache_age = 24 * 60 * 60 * 1000;      // 24 hours...
const batch_size = 50;

const nasdaq_symbols_uri = 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download';
const nyse_symbols_uri = 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download';
const amex_symbols_uri = 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download';

const yahoo_quotes_uri = 'http://finance.yahoo.com/d/quotes.csv';
const yahoo_quotes_flags = {
    'a': 'Ask',
    'a2': 'Average Daily Volume',
    'a5': 'Ask Size',
    'b': 'Bid',
    'b2': 'Ask (Real-time)',
    'b3': 'Bid (Real-time)',
    'b4': 'Book Value',
    'b6': 'Bid Size',
    'c': 'Change & Percent Change',
    'c1': 'Change',
    'c3': 'Commission',
    'c6': 'Change (Real-time)',
    'c8': 'After Hours Change (Real-time)',
    'd': 'Dividend/Share',
    'd1': 'Last Trade Date',
    'd2': 'Trade Date',
    'e': 'Earnings/Share',
    'e1': 'Error Indication (returned for symbol changed / invalid)',
    'e7': 'EPS Estimate Current Year',
    'e8': 'EPS Estimate Next Year',
    'e9': 'EPS Estimate Next Quarter',
    'f6': 'Float Shares',
    'g': 'Day’s Low',
    'h': 'Day’s High',
    'j': '52-week Low',
    'k': '52-week High',
    'g1': 'Holdings Gain Percent',
    'g3': 'Annualized Gain',
    'g4': 'Holdings Gain',
    'g5': 'Holdings Gain Percent (Real-time)',
    'g6': 'Holdings Gain (Real-time)',
    'i': 'More Info',
    'i5': 'Order Book (Real-time)',
    'j1': 'Market Capitalization',
    'j3': 'Market Cap (Real-time)',
    'j4': 'EBITDA',
    'j5': 'Change From 52-week Low',
    'j6': 'Percent Change From 52-week Low',
    'k1': 'Last Trade (Real-time) With Time',
    'k2': 'Change Percent (Real-time)',
    'k3': 'Last Trade Size',
    'k4': 'Change From 52-week High',
    'k5': 'Percebt Change From 52-week High',
    'l': 'Last Trade (With Time)',
    'l1': 'Last Trade (Price Only)',
    'l2': 'High Limit',
    'l3': 'Low Limit',
    'm': 'Day’s Range',
    'm2': 'Day’s Range (Real-time)',
    'm3': '50-day Moving Average',
    'm4': '200-day Moving Average',
    'm5': 'Change From 200-day Moving Average',
    'm6': 'Percent Change From 200-day Moving Average',
    'm7': 'Change From 50-day Moving Average',
    'm8': 'Percent Change From 50-day Moving Average',
    'n': 'Name',
    'n4': 'Notes',
    'o': 'Open',
    'p': 'Previous Close',
    'p1': 'Price Paid',
    'p2': 'Change in Percent',
    'p5': 'Price/Sales',
    'p6': 'Price/Book',
    'q': 'Ex-Dividend Date',
    'r': 'P/E Ratio',
    'r1': 'Dividend Pay Date',
    'r2': 'P/E Ratio (Real-time)',
    'r5': 'PEG Ratio',
    'r6': 'Price/EPS Estimate Current Year',
    'r7': 'Price/EPS Estimate Next Year',
    's': 'Symbol',
    's1': 'Shares Owned',
    's7': 'Short Ratio',
    't1': 'Last Trade Time',
    't6': 'Trade Links',
    't7': 'Ticker Trend',
    't8': '1 yr Target Price',
    'v': 'Volume',
    'v1': 'Holdings Value',
    'v7': 'Holdings Value (Real-time)',
    'w': '52-week Range',
    'w1': 'Day’s Value Change',
    'w4': 'Day’s Value Change (Real-time)',
    'x': 'Stock Exchange',
    'y': 'Dividend Yield'
};

run();

function run() {
    console.log('run...');

    getSymbolData('symbols.nyse').then(
        symbols => {
            console.log(symbols.length + ' symbols loaded');

            let batches = [];
            let start = 0;
            let end = 0;
            while (start < symbols.length) {
                let batch = [];
                end = start + batch_size;
                if (end > symbols.length) {end = symbols.length;}
                let sub = symbols.slice(start, end);
                sub.forEach(s => batch.push(encodeURIComponent(s.Symbol)));
                batches.push(batch.join('+'));
                start = end;
            }

            batches = batches.slice(0, 3);
            let promises = [];
            batches.forEach(batch => {
                let p = fetchQuote(batch, 'sl1rv');
                promises.push(p);
            });

            Promise.all(promises).then(
                data => {
                    console.log('data = ' + JSON.stringify(data));
                },
                error => {
                    handleError(error);
                }
            );
        },
        error => handleError(error)
    );
}

function handleError(error) {
    console.error(JSON.stringify(error));
}

function getSymbolData(type) {
    console.log('getSymbolData ' + type);
    return new Promise ((resolve, reject) => {
        readSymbolData(type).then(
            data => resolve(data),
            error => {
                console.log('getSymbolData: readSymbolData failed with ' + error);
                fetchSymbolData(type).then(
                    data => resolve(data),
                    error => reject(error)
                );
            }
        );
    });
}

/**
 * Pull symbol data from disk; return parsed JSON.
 *
 * @param type
 */
function readSymbolData(type) {
    console.log('readSymbolData ' + type);
    return new Promise((resolve, reject) => {
       let name = './cache/' + type + '.json';
       let now = Date.now();
       fs.stat(name).then(
           stats => {
               if (now - stats.atime.getTime() < max_cache_age) {
                   console.log('readSymbolData: read from cache');
                   fs.readFile(name, {}).then(
                       d => {
                           console.log('readSymbolData: data read, length: ' + d.length);
                           try {
                               let data = JSON.parse(d);
                               resolve(data);
                           }
                           catch(error) {
                               reject(error);
                           }
                       },
                       error => reject(error)
                   )
               }
               else {
                   reject('readSymbolData: Cache too old');
               }
           },
           error => reject(error)
       )
   });
}

function writeSymbolData(type, data) {
    console.log('writeSymbolData');
    return new Promise((resolve, reject) => {
        let name = './cache/' + type + '.json';
        let d = JSON.stringify(data);
        fs.writeFile(name, d, {})
            .then(() => resolve())
            .catch(error => reject(error));
    });
}

function fetchSymbolData(type) {
    console.log('fetchSymbolData ' + type);
    return new Promise((resolve, reject) => {
        let options = {
            method: 'GET'
        };

        if ('symbols.nasdaq' === type) {
            options.uri = nasdaq_symbols_uri;
        }
        else if ('symbols.nyse' === type) {
            options.uri = nyse_symbols_uri;
        }
        else if ('symbols.amex' === type) {
            options.uri = amex_symbols_uri;
        }
        else {
            throw('Unknown type ' + type);
        }

        request(options)
            .then((response) => {
                let data = processNasdaqCsvResponse(response);
                writeSymbolData(type, data).then(
                    () => resolve(data),
                    error => reject(error)
                );
            })
            .catch((error) => {
                reject(error);
            });
    });
}

/**
 * Accept a string of raw CSV data and a delimiter and
 * return an array of rows of cells, accounting for quoted
 * strings wrapping values with embedded delimiters. The
 * comma is the default delimiter.
 *
 * @param response
 * @param delim
 * @returns {Array}
 */
function processCsvResponse(response, delim = ',') {
    console.log('processCsvResponse');
    // Clean up the incoming data by removing carriage returns
    // and trailing line feeds, then split by line feeds...
    let inrows = response.replace(/\r/g, '\n').replace(/\n\n/g, '\n').replace(/\n$/g, '').split('\n');
    let outrows = [];
    while (inrows.length > 0) {
        let inrow = inrows.shift();
        if (!inrow) {break;}
        let outrow = [];
        outrows.push(outrow);
        let inStr = false;
        let buffer = '';
        for (let i = 0; i < inrow.length; i++) {
            let c = inrow.charAt(i);
            // Check for quote...
            if (c === '"') {
                inStr = !inStr;
            }
            else if (inStr || c !== delim) {
                buffer += c;
            }
            else {
                outrow.push(buffer.trim());
                buffer = '';
            }
        }

        // Flush the last cell of the row...
        outrow.push(buffer.trim());
    }

    return outrows;
}

/**
 * Convert CSV symbol data from NASDAQ into objects that
 * use the header row as keys. Note that the CSV data
 * includes a header row.
 *
 * @param response
 * @returns {Array}
 */
function processNasdaqCsvResponse(response) {
    console.log('processNasdaqCsvResponse');
    let rows = processCsvResponse(response);
    let result = [];
    let header = rows.shift();
    rows.forEach(row => {
        let r = {};
        result.push(r);
        let i;
        for(i = 0; i < header.length; i++) {
            if (!header[i]) {continue;}
            r[header[i]] = row[i];
        }
        if (i < row.length - 1) {console.error('extra cells');}
    });
    return result;
}

function fetchQuote(symbol, flags) {
    console.log('fetchQuote ' + symbol);
    return new Promise((resolve, reject) => {
        let options = {
            uri: yahoo_quotes_uri,
            method: 'GET',
            qs: {
                s: symbol,
                f: flags
            }
        };

        request(options)
            .then((response) => {
                let data = processYahooCsvResponse(response, flags);
                resolve(data);
            })
            .catch((error) => {
                reject(error);
            });
    });
}

function processYahooCsvResponse(response, flags) {
    let rows = processCsvResponse(response);
    let fields = unpackYahooFlags(flags);

    let results = [];
    rows.forEach(row => {
        let result = {};
        results.push(result);
        for (let i = 0; i < fields.length; i++) {
            let field = fields[i];
            result[yahoo_quotes_flags[field]] = row[i];
        }
    });

    return results;
}

/**
 * Unpack yahoo flag strings into arrays. This is
 * a bit tricky since flags can be either one or
 * two characters long.
 *
 * @param flags
 * @returns {Array}
 */
function unpackYahooFlags(flags) {
    let fields = [];
    for (let i = 0; i < flags.length; i++) {
        let f = flags.charAt(i);
        if (i < flags.length - 2) {
            f += flags.charAt(i + 1);
        }
        if (yahoo_quotes_flags[f]) {
            fields.push(f);
            i++;
        }
        else {
            f = flags.charAt(i);
            if (yahoo_quotes_flags[f]) {
                fields.push(f);
            }
            else {
                throw('Hmm, can\'t find flag ' + f)
            }
        }
    }
    return fields;
}