'use strict'

// Bare minimum implementation of a consumer that builds and maintains two read models.
// Each read model is initialized with aggregates fetched from the REST API.
// It is subsequently kept up-to-date with events consumed from the corresponding SSE channel.
// The events are merged into the read model with help of library 'json-merge-patch'.

import fetch from "node-fetch";
import EventSource from 'eventsource'
import Merger from 'json-merge-patch'

const HOST = 'http://localhost:3000'
const REVISION_HEADER = 'X-Revision'

async function bootstrap(path) {
    const response = await fetch(`${HOST}/${path}`)
    const revision = Number(response.headers.get(REVISION_HEADER)) || 0
    let records = await response.json()
    console.log("fetch:", records)
    return { revision, records }
}

function subscribe(path, { revision, records }) {
    const headers = { headers: { [REVISION_HEADER]: revision + 1 }}
    const eventSource = new EventSource(`${HOST}/${path}`, headers)
    eventSource.onmessage = event => {
        const patch = JSON.parse(event.data)
        console.log("patch:", patch)
        records = Merger.apply(records, patch)
        console.log("merge:", records)
    }
}

try {
    const persons = await bootstrap('persons')
    const locations = await bootstrap('locations')
    subscribe('person-events', persons)
    subscribe('location-events', locations)

} catch (e) {
    console.warn(e)
}
