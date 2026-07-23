FROM nathanhess/slurm:base-root

ARG SLURM_VERSION

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      curl \
      libdbus-1-dev \
      libhttp-parser-dev \
      libhwloc-dev \
      libjson-c-dev \
      libjwt-dev \
      liblz4-dev \
      libmunge-dev \
      libncurses-dev \
      libpam0g-dev \
      libreadline-dev \
      libsystemd-dev \
      libyaml-dev \
      pkg-config \
      python3 \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://download.schedmd.com/slurm/slurm-${SLURM_VERSION}.tar.bz2" \
      -o /tmp/slurm.tar.bz2 \
    && mkdir /tmp/slurm-source \
    && tar -xjf /tmp/slurm.tar.bz2 --strip-components=1 -C /tmp/slurm-source \
    && cd /tmp/slurm-source \
    && ./configure \
      --prefix=/usr \
      --sysconfdir=/etc/slurm \
      --localstatedir=/var \
      --runstatedir=/run \
    && make -j2 \
    && make install \
    && rm -rf /tmp/slurm-source /tmp/slurm.tar.bz2
