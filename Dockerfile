## docker build --build-ag
FROM alpine:3.6 as alpine-tools
MAINTAINER Quynh Dang

RUN apk update && apk add -U ffmpeg && mkdir /app
ADD vt-ingest /go/bin/vt-ingest
ENTRYPOINT /go/bin/vt-ingest
## from qdang+15151 user
ENV TOKEN REDACTED
ENV TERMS /app/terms.txt
ENV INDIR /app/input
ENV OUTDIR /app/output
## optional ENV WEBHOOK
