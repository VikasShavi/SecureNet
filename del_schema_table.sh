#!/bin/zsh


curl -X 'DELETE' \
  'http://localhost:9001/tables/logslabelledtwo?type=realtime' \
  -H 'accept: application/json'
  
curl -X 'DELETE' \
  'http://localhost:9001/schemas/logslabelledtwo' \
  -H 'accept: application/json'

