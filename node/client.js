'use strict'

import EventSource from 'eventsource'
import Merger from 'json-merge-patch'
import fetch from "node-fetch";

const HOST = 'http://localhost:3000'
const FROM_REVISION_HEADER = 'X-From-Revision'

try {
    const response = await fetch(`${HOST}/persons`)
    const revision = Number(response.headers.get(FROM_REVISION_HEADER)) || 0
    let persons = await response.json()
    console.log("fetch:", persons)

    const headers = { headers: { [FROM_REVISION_HEADER]: revision + 1 }}
    const eventSource = new EventSource(`${HOST}/person-events`, headers)
    eventSource.onmessage = event => {
        let patch = JSON.parse(event.data)
        console.log("patch:", patch)
        persons = Merger.apply(persons, patch)
        console.log("merge:", persons)
    }
} catch (e) {
    console.warn(e)
}
