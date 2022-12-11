#!/bin/sh
curl -X POST  -H 'Content-Type: application/json' -d '{"name":"Hans","location":"Berlin"}' http://localhost:3000/persons
curl -X POST  -H 'Content-Type: application/json' -d '{"name":"Inge","location":"Munich"}' http://localhost:3000/persons
curl -X PATCH -H 'Content-Type: application/json' -d '{"spouseId":2}' http://localhost:3000/persons/1
curl -X PATCH -H 'Content-Type: application/json' -d '{"location":"Berlin","spouseId":1}' http://localhost:3000/persons/2
curl -X PATCH -H 'Content-Type: application/json' -d '{"location":null,"spouseId":null}' http://localhost:3000/persons/1
curl -X PATCH -H 'Content-Type: application/json' -d '{"spouseId":null}' http://localhost:3000/persons/2
curl -X DELETE http://localhost:3000/persons/1

echo "\n===== person aggregate ====="
curl http://localhost:3000/persons

echo '\n===== location aggregate ====='
curl http://localhost:3000/locations

echo '\n===== location events ====='
curl -N -H 'X-From-Revision: 1' http://localhost:3000/location-events
