FROM alpine:3.15.5

RUN apk update && \
    apk add --no-cache inotify-tools=3.20.11.0-r0 \
    aws-cli=1.19.105-r0 \
    ca-certificates

COPY capture.sh /usr/bin/
RUN chmod +x /usr/bin/capture.sh
RUN adduser -u 1000 --gecos '' --disabled-password --no-create-home myuser
USER myuser
ENTRYPOINT ["/usr/bin/capture.sh"]
