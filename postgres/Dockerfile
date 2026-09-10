FROM postgres:17

RUN apt-get update \
 && apt-get install -y --no-install-recommends locales tzdata \
 && sed -i 's/^# *fi_FI.UTF-8 UTF-8/fi_FI.UTF-8 UTF-8/' /etc/locale.gen \
 && locale-gen \
 && rm -rf /var/lib/apt/lists/*

ENV LANG=fi_FI.UTF-8
ENV LANGUAGE=fi_FI:fi
ENV LC_ALL=fi_FI.UTF-8
ENV TZ=Europe/Helsinki
