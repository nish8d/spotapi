# Flink plus the three connector jars, fetched when the image is BUILT. A job
# that needs the network at submission time fails somewhere far from its
# cause; a missing jar here fails the build instead. Versions come from
# jars.txt, verified against the Flink 1.20 suffix in M0.
FROM flink:1.20-java17

COPY jars.txt /tmp/jars.txt
RUN grep -v '^#' /tmp/jars.txt | grep . | xargs -n1 wget -q -P /opt/flink/lib/ \
 && rm /tmp/jars.txt \
 # The checkpoint volume is mounted here. A fresh named volume copies the
 # ownership of the directory it is mounted over, and Flink runs as `flink`.
 && mkdir -p /opt/flink/checkpoints \
 && chown flink:flink /opt/flink/checkpoints

# Baked in rather than mounted: it changes far less often than the SQL does.
COPY --chmod=755 submit.sh /opt/flink/bin/spot-submit.sh
