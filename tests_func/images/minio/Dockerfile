FROM minio/mc:RELEASE.2021-02-19T05-34-40Z
FROM minio/minio:RELEASE.2021-03-01T04-20-55Z

COPY --from=0 /usr/bin/mc /usr/bin/mc

ENV MINIO_ACCESS_KEY S3_ACCESS_KEY
ENV MINIO_SECRET_KEY S3_SECRET_KEY

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

HEALTHCHECK --interval=30s --timeout=5s \
    CMD /usr/bin/healthcheck.sh

EXPOSE $S3_PORT

CMD ["server", "/export"]
