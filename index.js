const express = require('express');
const actuator = require('express-actuator');
const EventSource = require('eventsource');
const Denque = require("denque");

const app = express();
app.use(actuator());

const PORT = 3000;

const URL = 'https://stream.wikimedia.org/v2/stream/recentchange';
const eventSource = new EventSource(URL);

eventSource.onopen = () => {
    console.info('Opened connection.');
};

eventSource.onerror = (event) => {
    console.error('Encountered error', event);
};

const wikiRecords = new Denque();

eventSource.onmessage = (event) => {
    const data = JSON.parse(event.data);

    wikiRecords.push({
        wiki: data.wiki,
        title: data.title
    });

    if (wikiRecords.size() >= 100) {
        wikiRecords.shift()
    }
};

const getEvents = async (write, f, distinct=false, condition) => {
    let lastEvent = null;

    const duplicationCheck = new Set()

    while (true) {
        if (wikiRecords.length && lastEvent !== wikiRecords.peekFront()) {
            let startIndex = 0;

            const records = wikiRecords.toArray();

            if (lastEvent != null) {
                startIndex = records.indexOf(lastEvent);

                if (startIndex < 0) {
                    startIndex = 0;
                }
            }

            for (let i = startIndex + 1; i < records.length; i++) {
                const data = records[i];

                if (!condition || condition(data)) {
                    const value = f(data);

                    if (distinct) {
                        if (duplicationCheck.has(value)) {
                            continue;
                        }

                        duplicationCheck.add(value);
                    }

                    write(value);
                }

                lastEvent = data;
            }
        }

        await new Promise(resolve => setTimeout(resolve, 100));
    }
}

const setupResponse = (res) => {
    res.set({
        'Cache-Control': 'no-cache',
        'Content-Type': 'text/event-stream',
        'Connection': 'keep-alive'
    });

    res.flushHeaders();

}

app.get('/titles', async function(req, res) {
    setupResponse(res);

    return getEvents((value) => {
        res.write(`data: ${value}\n\n`);
    },(value) => value.title);
});

app.get('/titles/:wiki', async function(req, res) {
    setupResponse(res);

    return getEvents((value) => {
        res.write(`data: ${value}\n\n`);
    },(value) => value.title, false, (value) => value.wiki === req.params.wiki);
});

app.get('/wikis', async function(req, res) {
    setupResponse(res);

    return getEvents((value) => {
        res.write(`data: ${value}\n\n`);
    },(value) => value.wiki, true);
});

app.listen(PORT, function() {
  console.log('Server listening on http://localhost:' + PORT);
});