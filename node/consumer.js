'use strict'

import fetch from "node-fetch";
import EventSource from 'eventsource'
import Merger from 'json-merge-patch'

const HOST = 'http://localhost:3000'
const REVISION_HEADER = 'X-Revision'

// Bare minimum implementation of a consumer that builds and maintains a read model.
// The read model ("persons") is initialized with person aggregates fetched from the REST API.
// It is subsequently kept up-to-date with person events consumed from the corresponding SSE channel.
// The events are merged into the read model with help of library 'json-merge-patch'.
try {
    const response = await fetch(`${HOST}/persons`)
    const revision = Number(response.headers.get(REVISION_HEADER)) || 0
    let persons = await response.json()
    console.log("fetch:", persons)

    const headers = { headers: { [REVISION_HEADER]: revision + 1 }}
    const eventSource = new EventSource(`${HOST}/person-events`, headers)
    eventSource.onmessage = event => {
        const patch = JSON.parse(event.data)
        console.log("patch:", patch)
        persons = Merger.apply(persons, patch)
        console.log("merge:", persons)
    }
} catch (e) {
    console.warn(e)
}
