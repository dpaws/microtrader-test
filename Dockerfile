FROM dockerproductionaws/microtrader-dev
MAINTAINER Justin Menga <justin.menga@gmail.com>
LABEL application=microtrader-test

# Copy just the POM first
COPY pom.xml /app/
WORKDIR /app

# Install dependencies
RUN mvn clean install

# Copy application config
COPY src/conf/*.tmpl /etc/confd/templates/
COPY src/conf/*.toml /etc/confd/conf.d/

# Copy the application source
COPY src /app/src
