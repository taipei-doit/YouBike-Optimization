# Clojure tabular ETL batch image (Temurin 17 — no host JDK required)
FROM eclipse-temurin:17-jdk-jammy

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl rlwrap \
    && rm -rf /var/lib/apt/lists/*

# Official POSIX installer into /usr/local
RUN curl -sL https://github.com/clojure/brew-install/releases/latest/download/posix-install.sh \
    -o /tmp/posix-install.sh \
    && chmod +x /tmp/posix-install.sh \
    && /tmp/posix-install.sh --prefix /usr/local \
    && rm /tmp/posix-install.sh

WORKDIR /work/clj-etl

# Prefetch deps (layer cache)
COPY deps.edn ./
RUN clojure -P -M:run

COPY src ./src

ENV CLJ_CONFIG=/tmp/.clojure

ENTRYPOINT ["clojure", "-M:run"]
CMD ["--dates", "20230305", "--staging", "../staging", "--population-dir", "../input/data/population"]
