FROM php:cli-bullseye

# install phpunit for test
RUN curl -fsSL -o /usr/local/bin/phpunit https://phar.phpunit.de/phpunit-10.phar && \
    chmod +x /usr/local/bin/phpunit

# install pdo-pgsql extension
RUN apt-get update && \
    apt-get install -y libpq-dev && \
    docker-php-ext-install pdo_pgsql

CMD ["php", "-a"]
